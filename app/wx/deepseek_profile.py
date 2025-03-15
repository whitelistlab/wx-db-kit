from typing import List, Dict
import json
from datetime import datetime, timedelta
import httpx
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import re
import jieba

class DeepSeekProfileAnalyzer:
    def __init__(self, mongodb_uri: str = "mongodb://localhost:27017/", database_name: str = "t"):
        self.client = MongoClient(mongodb_uri)
        self.db = self.client[database_name]
        self.msg_collection = self.db.messages
        self.contacts_collection = self.db.contacts
        load_dotenv()
        self.api_key = os.getenv("DEEPSEEK_API_KEY")  # 修改环境变量名
        self.api_url = "https://api.deepseek.com/v1/chat/completions"
        
        # 扩展垃圾信息关键词
        self.spam_keywords = [
            # 寒暄类
            '你好', '嗨', '在吗', '在不在', '您好', '早上好', '晚安', '谢谢', '感谢', '不客气',
            # 填充词
            '嗯', '哦', '额', '啊', '哈哈', '呵呵',
            # 回应词
            '是的', '好的', '嗯嗯', '可以', '没问题', '收到', '1' 
            # 系统消息
            '欢迎加入群聊', '本群禁止广告', '红包已领取'
        ]
        
        # 电商领域停用词
        self.stopwords = set([
            '的', '了', '和', '是', '就', '都', '而', '及', '与', '这', '那', '你', '我', '他',
            '也', '但', '又', '或', '如果', '因为', '所以', '只是', '不过', '可以', '没有'
        ])
        
        # 扩展特殊字符集
        self.special_chars = set(['/', '\\', '，', '。', '：', '；', '！', '？', '、', '~', '@', '#', '$',
                               '%', '^', '&', '*', '(', ')', '-', '_', '+', '=', '<', '>', '"', '\'',
                               '[', ']', '{', '}', '|', '`', '（', '）', '【', '】', '《', '》'])
    
    def filter_spam_content(self, content: str) -> bool:
        """过滤垃圾内容
        返回True表示是正常内容，False表示是垃圾内容
        """
        # 预处理：分词并过滤停用词
        words = [word for word in jieba.cut(content) if word not in self.stopwords]
        processed_content = ''.join(words)

        # 检查消息内容是否完全等于垃圾关键词
        if processed_content.strip() in self.spam_keywords:
            return False

        # 处理空内容情况
        if len(processed_content) == 0:
            return False
            
        # 多层级重复检测：原始内容和处理后内容双重检测
        raw_repeat_ratio = len(set(content)) / max(len(content), 1)
        processed_repeat_ratio = len(set(processed_content)) / max(len(processed_content), 1)
        
        if raw_repeat_ratio < 0.2 or processed_repeat_ratio < 0.15:
            return False

        # 动态内容长度检测
        valid_length = 3 < len(processed_content) < 800
        if not valid_length:
            return False
        
        return True

    def clean_text(self, text: str) -> str:
        """清理文本内容"""
        # 去除特殊字符
        for char in self.special_chars:
            text = text.replace(char, ' ')
        
        # 去除多余空白
        text = re.sub(r'\s+', ' ', text)
        
        # 去除URL
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        
        # 去除表情符号
        text = re.sub(r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF]', '', text)
        
        return text.strip()
    
    async def analyze_user_profile(self, nickname: str, days: int = None) -> Dict:
        """分析用户画像"""
        # 获取用户信息
        contact = self.contacts_collection.find_one({"nickname": nickname})
        if not contact:
            return {"error": f"未找到联系人: {nickname}"}
            
        str_talker = contact['username']
        
        # 构建查询条件
        query = {
            "$or": [
                # 私聊消息
                {"str_talker": str_talker, "type": 1},
                # 群聊消息
                {"user_name": str_talker, "type": 1}
            ]
        }
        
        # 仅在指定days参数时添加时间范围限制
        if days is not None:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            query["create_time"] = {"$gte": start_date, "$lte": end_date}
            print(f"时间范围: {start_date} 至 {end_date}")
        
        # 获取私聊和群聊消息
        messages = list(self.msg_collection.find(query).sort("create_time", 1))
        
        if not messages:
            return {"error": f"未找到消息记录: {str_talker}"}
        print(f"找到消息数量: {len(messages)}")
        # 过滤和清理消息
        filtered_messages = []
        
        for msg in messages:
            content = msg.get('str_content', '')
            # 使用clean_text方法清理文本
            cleaned_content = self.clean_text(content)
            
            if cleaned_content and self.filter_spam_content(cleaned_content):
                msg['str_content'] = cleaned_content
                filtered_messages.append(msg)
                
        
        # 分段处理消息
        message_segments = self._segment_messages(filtered_messages)
        all_analysis = []
        
        # 分析每个消息段
        async with httpx.AsyncClient() as client:
            for segment in message_segments:
                analysis = await self._analyze_segment(segment, client)
                if analysis:
                    all_analysis.append(analysis)
        
        # 整合分析结果
        return self._merge_analysis_results(all_analysis)
    
    def _segment_messages(self, messages: List[Dict], max_tokens: int = 4000) -> List[List[Dict]]:
        """将消息分段，确保每段不超过token限制"""
        segments = []
        current_segment = []
        current_tokens = 0
        
        for msg in messages:
            content = msg.get('str_content', '')
            # 粗略估计token数量
            tokens = len(content) * 1.5
            
            if current_tokens + tokens > max_tokens:
                if current_segment:
                    segments.append(current_segment)
                current_segment = [msg]
                current_tokens = tokens
            else:
                current_segment.append(msg)
                current_tokens += tokens
        
        if current_segment:
            segments.append(current_segment)
        
        return segments
    
    async def _analyze_segment(self, messages: List[Dict], client: httpx.AsyncClient) -> Dict:
        """分析单个消息段"""
        # 构建对话内容
        conversation = self._format_messages(messages)
        
        # 构建 prompt
        prompt = f"""你是一个用户画像专家，分析以下聊天记录，生成该用户的电商相关画像，包括：
1. 购物偏好（价格敏感度、品牌态度、产品品类等）
2. 消费行为（理性/冲动、决策周期、参考因素等）
3. 购物动机（功能性需求、情感需求等）
4. 其他特征（活跃度、客服交互方式、最适合分享的内容类型、潜在的销售机会等）

聊天记录：
{conversation}

请以结构化的JSON格式输出分析结果。"""

        try:
            response = await client.post(
                self.api_url,
                headers={"Authorization": f"Bearer {self.api_key}"},
                json={
                    "model": "deepseek-chat",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.7
                },
                timeout=30.0
            )
            
            if response.status_code == 200:
                result = response.json()
                # 提取 JSON 字符串并清理
                content = result['choices'][0]['message']['content']
                # 移除可能的 ```json 和 ``` 标记
                content = content.replace('```json\n', '').replace('\n```', '')
                return json.loads(content)
            else:
                print(f"API请求失败: {response.status_code}")
                print(f"错误详情: {response.text}")
                return None
                
        except Exception as e:
            print(f"分析过程出错: {str(e)}")
            print(f"错误类型: {type(e)}")  # 添加这行来显示错误类型
            return None
    
    def _format_messages(self, messages: List[Dict]) -> str:
        """格式化消息记录"""
        formatted = []
        for msg in messages:
            time_str = msg['create_time'].strftime("%Y-%m-%d %H:%M:%S")
            role = "用户" if not msg['is_sender'] else "商家"
            content = msg.get('str_content', '')
            formatted.append(f"[{time_str}] {role}: {content}")
        
        return "\n".join(formatted)
    
    def _merge_analysis_results(self, analysis_results: List[Dict]) -> Dict:
        """合并多段分析结果"""
        if not analysis_results:
            return {"error": "无有效分析结果"}
            
        merged = {
            "购物偏好": {},
            "消费行为": {},
            "购物动机": {},
            "其他特征": {}
        }
        
        # 合并各个维度的分析结果
        for result in analysis_results:
            try:
                for category in merged.keys():
                    if category in result:
                        # 确保result[category]是字典类型
                        if isinstance(result[category], dict):
                            merged[category].update(result[category])
                        elif isinstance(result[category], str):
                            # 如果是字符串，将其作为值存储
                            merged[category][f'value_{len(merged[category])}'] = result[category]
                        elif isinstance(result[category], list):
                            # 如果是列表，将每个元素作为单独的值存储
                            for i, item in enumerate(result[category]):
                                merged[category][f'item_{len(merged[category])}'] = item
            except Exception as e:
                print(f"合并分析结果时出错: {str(e)}")
                continue
        
        return merged


if __name__ == "__main__":
    import asyncio
    
    async def main():
        analyzer = DeepSeekProfileAnalyzer(database_name='my_wechat')
        profile = await analyzer.analyze_user_profile("Ann")
        print(json.dumps(profile, ensure_ascii=False, indent=2))
    
    asyncio.run(main())