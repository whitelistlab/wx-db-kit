import jieba
import pandas as pd
from collections import Counter
from typing import List, Dict, Any
import re
from sklearn.feature_extraction.text import TfidfVectorizer

class UserAnalyzer:
    def __init__(self):
        # 预定义的关键词字典
        self.interest_keywords = {
            '美妆': ['化妆品', '口红', '护肤', '美妆', '面膜'],
            '数码': ['手机', '电脑', '相机', '耳机', '平板'],
            '服装': ['衣服', '裤子', '鞋子', '包包', '配饰'],
            '母婴': ['奶粉', '尿布', '玩具', '童装', '婴儿'],
            '运动': ['健身', '跑步', '瑜伽', '篮球', '游泳'],
        }
        
        self.personal_tag_patterns = {
            '性别': {
                '女': r'(女士|小姐|美女|姐姐|她)',
                '男': r'(先生|男士|帅哥|哥哥|他)'
            },
            '年龄段': {
                '学生': r'(学生|上学|考试|作业)',
                '青年': r'(工作|加班|职场|结婚)',
                '中年': r'(孩子|家庭|育儿)'
            },
            '消费能力': {
                '高': r'(大牌|奢侈品|专柜|贵|质量)',
                '中': r'(性价比|优惠|打折|促销)',
                '低': r'(便宜|实惠|价格|便宜)',
            }
        }
        
        # 初始化分词器
        jieba.initialize()
    
    def preprocess_text(self, text: str) -> str:
        """预处理文本"""
        # 移除特殊字符和多余的空白
        text = re.sub(r'[^\w\s]', '', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    
    def extract_personal_tags(self, chat_records: List[str]) -> Dict[str, str]:
        """提取个人标签"""
        combined_text = ' '.join(chat_records)
        tags = {}
        
        for category, patterns in self.personal_tag_patterns.items():
            max_count = 0
            selected_tag = None
            
            for tag, pattern in patterns.items():
                count = len(re.findall(pattern, combined_text))
                if count > max_count:
                    max_count = count
                    selected_tag = tag
            
            tags[category] = selected_tag if max_count > 0 else '未知'
        
        return tags
    
    def analyze_interests(self, chat_records: List[str]) -> Dict[str, float]:
        """分析用户兴趣"""
        # 将聊天记录合并并分词
        processed_texts = []
        for text in chat_records:
            words = jieba.cut(self.preprocess_text(text))
            processed_texts.append(' '.join(words))
        
        # 统计兴趣关键词
        interest_scores = {category: 0 for category in self.interest_keywords}
        
        for text in processed_texts:
            for category, keywords in self.interest_keywords.items():
                for keyword in keywords:
                    if keyword in text:
                        interest_scores[category] += 1
        
        # 归一化兴趣分数
        total_score = sum(interest_scores.values()) or 1
        interest_scores = {k: v/total_score for k, v in interest_scores.items()}
        
        return interest_scores
    
    def generate_user_profile(self, chat_records: List[str]) -> Dict[str, Any]:
        """生成用户画像"""
        personal_tags = self.extract_personal_tags(chat_records)
        interests = self.analyze_interests(chat_records)
        
        # 按兴趣分数排序
        sorted_interests = dict(sorted(
            interests.items(), 
            key=lambda x: x[1], 
            reverse=True
        ))
        
        return {
            "个人标签": personal_tags,
            "兴趣偏好": sorted_interests
        } 