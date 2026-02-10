import pandas as pd
import jieba
import jieba.posseg as pseg
import re
import json
from collections import Counter

def load_data():
    videos = pd.read_csv('dataset_videos.csv')
    comments = pd.read_csv('dataset_comments.csv')
    
    texts = []
    # 视频标题和简介
    if 'title' in videos.columns:
        texts.extend(videos['title'].dropna().tolist())
    if 'description' in videos.columns:
        texts.extend(videos['description'].dropna().tolist())
        
    # 评论内容
    if 'content' in comments.columns:
        texts.extend(comments['content'].dropna().tolist())
        
    return texts

def extract_potential_stocks(texts):
    # 加载已有的股票映射（如果有）
    known_stocks = set()
    try:
        with open('stock_map.json', 'r', encoding='utf-8') as f:
            mapping = json.load(f)
            known_stocks = set(mapping.keys())
    except:
        pass
        
    potential_stocks = []
    
    # 关键词上下文提取
    keywords = ['买', '卖', '入', '出', '板', '涨', '跌', '加仓', '减仓', '持有', '格局']
    
    for text in texts:
        # 简单的清理
        text = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9]', ' ', text)
        
        words = pseg.cut(text)
        word_list = []
        for w, flag in words:
            if flag.startswith('n') and len(w) >= 2 and len(w) <= 5: # 名词, 2-5个字
                word_list.append(w)
            elif w in keywords:
                word_list.append(w)
            # 也可以保留全名 (e.g. 赛伍技术) 即使jieba没分出来，但jieba通常能分出大部分
            
        # 寻找上下文中的名词
        # 这里简单点：直接统计所有名词的频率，人工筛选最快
        for w in word_list:
            if w not in keywords:
                potential_stocks.append(w)
                
    # 统计频率
    counter = Counter(potential_stocks)
    
    print("Top 50 潜在股票名词 (按频率排序):")
    results = []
    for word, count in counter.most_common(100):
        # 简单的过滤：排除一些明显不是股票的词
        if word in ['视频', '大家', '朋友', '个人', '观点', '参考', '内容', '链接', '评论', '大佬', '老师']:
            continue
        
        status = "已知" if word in known_stocks else "未知"
        print(f"{word}: {count} ({status})")
        if status == "未知":
            results.append(word)
            
    return results

if __name__ == "__main__":
    texts = load_data()
    unknowns = extract_potential_stocks(texts)
    
    # 保存未知的高频词到文件，方便后续处理
    with open('unknown_stocks.txt', 'w', encoding='utf-8') as f:
        for w in unknowns:
            f.write(w + '\n')
    print(f"\n已保存 {len(unknowns)} 个未知潜在股票词到 unknown_stocks.txt")
