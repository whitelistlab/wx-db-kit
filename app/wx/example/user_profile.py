import json
import os
import sys
from collections import Counter
from datetime import datetime
from typing import Dict, List, Tuple
import jieba
import numpy as np
from snownlp import SnowNLP

# 将项目根目录添加到 sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

from app.util.protocbuf import MessageBytesExtra
from app.DataBase import msg_db, MsgType

class UserProfile:
    def __init__(self, wxid: str):
        self.wxid = wxid
        self.messages = []
        self._load_messages()
        
        # 加载停用词和自定义词典
        self.stopwords = self._load_stopwords()
        jieba.load_userdict('./app/data/new_words.txt')
    
    def _load_messages(self):
        """加载用户的聊天记录"""
        self.messages = msg_db.get_messages(self.wxid)
    
    def _load_stopwords(self) -> set:
        """加载停用词表"""
        stopwords = set()
        stopwords_files = [
            './app/data/stopwords.txt',
            './app/resources/data/stopwords.txt'
        ]
        
        for file_path in stopwords_files:
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    stopwords.update(f.read().splitlines())
        return stopwords
    
    def analyze_activity_pattern(self) -> Dict:
        """分析用户活跃时间模式"""
        hour_stats = Counter()
        weekday_stats = Counter()
        
        for msg in self.messages:
            timestamp = msg[5]  # CreateTime字段
            dt = datetime.fromtimestamp(timestamp)
            hour_stats[dt.hour] += 1
            weekday_stats[dt.strftime('%A')] += 1
        
        return {
            'peak_hours': dict(hour_stats.most_common(3)),
            'active_days': dict(weekday_stats.most_common())
        }
    
    def analyze_social_traits(self) -> Dict:
        """分析用户社交特征"""
        msg_types = Counter()
        response_times = []
        last_time = None
        
        for msg in self.messages:
            msg_type = msg[2]  # Type字段
            is_sender = msg[4]  # IsSender字段
            timestamp = msg[5]  # CreateTime字段
            
            msg_types[msg_type] += 1
            
            if last_time and not is_sender:
                response_time = timestamp - last_time
                if response_time < 3600:  # 只统计1小时内的回复
                    response_times.append(response_time)
            last_time = timestamp
        
        avg_response_time = np.mean(response_times) if response_times else 0
        
        return {
            'message_type_distribution': dict(msg_types),
            'avg_response_time': avg_response_time,
            'total_messages': len(self.messages)
        }
    
    def analyze_emotional_tendency(self) -> Dict:
        """分析用户情感倾向"""
        sentiments = []
        
        for msg in self.messages:
            if msg[2] == MsgType.TEXT:  # 只分析文本消息
                content = msg[7]  # StrContent字段
                try:
                    sentiment = SnowNLP(content).sentiments
                    sentiments.append(sentiment)
                except:
                    continue
        
        return {
            'avg_sentiment': np.mean(sentiments) if sentiments else 0.5,
            'sentiment_distribution': {
                'positive': sum(1 for s in sentiments if s > 0.7),
                'neutral': sum(1 for s in sentiments if 0.3 <= s <= 0.7),
                'negative': sum(1 for s in sentiments if s < 0.3)
            }
        }
    
    def analyze_interests(self) -> Dict:
        """分析用户兴趣爱好"""
        text = ''
        shared_content = Counter()
        
        for msg in self.messages:
            msg_type = msg[2]  # Type字段
            content = msg[7]  # StrContent字段
            bytes_extra = msg[6]  # BytesExtra字段
            
            if msg_type == MsgType.TEXT:
                text += content + '\n'
            elif msg_type == 49:  # 分享内容
                try:
                    msgbytes = MessageBytesExtra()
                    msgbytes.ParseFromString(bytes_extra)
                    for tmp in msgbytes.message2:
                        if tmp.field1 == 1:  # 分享标题
                            shared_content[tmp.field2] += 1
                except:
                    continue
        
        # 关键词提取
        words = jieba.cut(text)
        word_count = Counter(w for w in words if len(w) > 1 and w not in self.stopwords)
        
        return {
            'top_keywords': dict(word_count.most_common(20)),
            'shared_content': dict(shared_content.most_common(10))
        }
    
    def generate_profile(self) -> Dict:
        """生成完整的用户画像"""
        return {
            'personal_tags': {
                'activity_pattern': self.analyze_activity_pattern(),
                'social_traits': self.analyze_social_traits(),
                'emotional_tendency': self.analyze_emotional_tendency()
            },
            'interests': self.analyze_interests()
        }

    def show_profile(self):
        """展示用户画像"""
        profile = self.generate_profile()
        print(json.dumps(profile, ensure_ascii=False, indent=4))

def main():
    # 示例使用
    wxid = input('请输入要分析的用户wxid：')
    profiler = UserProfile(wxid)
    profile = profiler.generate_profile()
    
    # 打印分析结果
    print('\n=== 用户画像分析结果 ===')
    print('\n== 个人标签 ==')
    print('\n= 活跃规律 =')
    print(f"高峰时段：{profile['personal_tags']['activity_pattern']['peak_hours']}")
    print(f"活跃天数分布：{profile['personal_tags']['activity_pattern']['active_days']}")
    
    print('\n= 社交特征 =')
    print(f"消息类型分布：{profile['personal_tags']['social_traits']['message_type_distribution']}")
    print(f"平均回复时间：{profile['personal_tags']['social_traits']['avg_response_time']:.2f}秒")
    print(f"总消息数：{profile['personal_tags']['social_traits']['total_messages']}")
    
    print('\n= 情感倾向 =')
    print(f"平均情感值：{profile['personal_tags']['emotional_tendency']['avg_sentiment']:.2f}")
    print(f"情感分布：{profile['personal_tags']['emotional_tendency']['sentiment_distribution']}")
    
    print('\n== 兴趣爱好 ==')
    print('\n= 高频关键词 =')
    for word, count in profile['interests']['top_keywords'].items():
        print(f"{word}: {count}")
    
    print('\n= 分享内容分析 =')
    for content, count in profile['interests']['shared_content'].items():
        print(f"{content}: {count}")

if __name__ == '__main__':
    main()