import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, time
from typing import Any, Dict, Optional, Tuple
import pandas as pd


logger = logging.getLogger(__name__)


@dataclass
class BayesianParams:
    alpha: float
    beta: float

    @property
    def posterior_mean(self) -> float:
        denom = self.alpha + self.beta
        if denom <= 0:
            return 0.5
        return self.alpha / denom


class CreatorCredibilityScorer:
    def __init__(
        self,
        state_path: str = "data/blogger_bayes_state.json",
        alpha_prior: float = 2.0,
        beta_prior: float = 2.0,
    ) -> None:
        self.state_path = state_path
        self.alpha_prior = float(alpha_prior)
        self.beta_prior = float(beta_prior)
        self._state: Dict[str, BayesianParams] = {}
        self._loaded = False

    def load_state(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        if not os.path.exists(self.state_path):
            self._state = {}
            return
        try:
            with open(self.state_path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            state: Dict[str, BayesianParams] = {}
            for author, params in (raw or {}).items():
                if not isinstance(params, dict):
                    continue
                alpha = float(params.get("alpha", self.alpha_prior))
                beta = float(params.get("beta", self.beta_prior))
                state[str(author)] = BayesianParams(alpha=alpha, beta=beta)
            self._state = state
        except Exception as e:
            logger.warning("Failed to load bayesian state: %s", e)
            self._state = {}

    def save_state(self) -> None:
        try:
            os.makedirs(os.path.dirname(self.state_path) or ".", exist_ok=True)
            raw = {k: {"alpha": v.alpha, "beta": v.beta} for k, v in self._state.items()}
            with open(self.state_path, "w", encoding="utf-8") as f:
                json.dump(raw, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning("Failed to save bayesian state: %s", e)

    def _time_bucket_score(self, t: time) -> float:
        pre_market_end = time(9, 25)
        morning_end = time(11, 30)
        afternoon_start = time(13, 0)
        afternoon_end = time(14, 50)
        tail_end = time(15, 0)

        if t < pre_market_end:
            return 100.0
        if pre_market_end <= t <= morning_end:
            return 85.0
        if afternoon_start <= t <= afternoon_end:
            return 85.0
        if afternoon_end < t <= tail_end:
            return 80.0
        return 40.0

    def _parse_time_from_row(self, row: Dict[str, Any]) -> Optional[time]:
        candidates = []
        for k in ("publish_time", "date", "datetime", "created_at"):
            v = row.get(k)
            if v is None:
                continue
            s = str(v).strip()
            if not s or s.lower() == "nan":
                continue
            candidates.append(s)

        for s in candidates:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%H:%M:%S", "%H:%M"):
                try:
                    dt = datetime.strptime(s, fmt)
                    return dt.time()
                except Exception:
                    continue
            if " " in s:
                tail = s.split(" ", 1)[1].strip()
                for fmt in ("%H:%M:%S", "%H:%M"):
                    try:
                        dt = datetime.strptime(tail, fmt)
                        return dt.time()
                    except Exception:
                        continue
        return None

    def get_posterior_params(self, author_name: str) -> BayesianParams:
        self.load_state()
        author = str(author_name or "Unknown")
        return self._state.get(author, BayesianParams(alpha=self.alpha_prior, beta=self.beta_prior))

    def update(self, author_name: str, success: bool, weight: float = 1.0) -> BayesianParams:
        self.load_state()
        author = str(author_name or "Unknown")
        params = self._state.get(author, BayesianParams(alpha=self.alpha_prior, beta=self.beta_prior))
        w = max(0.0, float(weight))
        if success:
            params = BayesianParams(alpha=params.alpha + w, beta=params.beta)
        else:
            params = BayesianParams(alpha=params.alpha, beta=params.beta + w)
        self._state[author] = params
        return params

    def score_row(self, row: Dict[str, Any]) -> Tuple[float, float]:
        author = str(row.get("author_name", "Unknown"))
        t = self._parse_time_from_row(row)
        time_score = self._time_bucket_score(t) if t is not None else 60.0
        posterior = self.get_posterior_params(author).posterior_mean * 100.0
        credibility = 0.4 * time_score + 0.6 * posterior
        return float(round(credibility, 2)), float(round(posterior, 2))

    def add_scores_to_signals_df(self, df_signals: pd.DataFrame) -> pd.DataFrame:
        if df_signals is None or df_signals.empty:
            return df_signals

        self.load_state()

        def _to_dict(row: pd.Series) -> Dict[str, Any]:
            return row.to_dict()

        scores = []
        posteriors = []
        for _, r in df_signals.iterrows():
            s, p = self.score_row(_to_dict(r))
            scores.append(s)
            posteriors.append(p)

        out = df_signals.copy()
        out["credibility_score"] = scores
        out["posterior_win_rate"] = posteriors
        return out

