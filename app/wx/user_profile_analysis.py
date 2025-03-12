import re
from datetime import datetime
from typing import Dict, List

import jieba
from pymongo import MongoClient
from collections import Counter

class UserProfileAnalyzer:
    def __init__(self, mongodb_uri: str = "mongodb://localhost:27017/", database_name: str = "wechat_msg"):
        self.client = MongoClient(mongodb_uri)
        self.db = self.client[database_name]
        self.msg_collection = self.db.messages
        self.contact_collection = self.db.contacts
        self.chatroom_collection = self.db.chatrooms
        
        # 扩展停用词和特殊字符集
        self.stopwords = set(['的', '了', '和', '是', '就', '都', '而', '及', '与', '这', '那', '你', '我', '他',
                           '也', '但', '又', '或', '如果', '因为', '所以', '只是', '不过', '可以', '没有',
                           '一个', '现在', '这个', '那个', '什么', '这样', '那样', '为了', '这些', '那些'])
        self.special_chars = set(['/', '\\', '，', '。', '：', '；', '！', '？', '、', '~', '@', '#', '$',
                               '%', '^', '&', '*', '(', ')', '-', '_', '+', '=', '<', '>', '"', '\'',
                               '[', ']', '{', '}', '|', '`'])
        
        # 扩展电商相关关键词
        self.shopping_keywords = {
            '询价': ['多少钱', '价格', '优惠', '便宜', '促销', '打折', '降价', '特价', '团购', '秒杀'],
            '购买意向': ['想买', '准备买', '考虑买', '推荐', '哪个好', '求推荐', '求建议', '怎么样', '靠谱吗'],
            '支付': ['付款', '支付', '转账', '下单', '订单', '微信支付', '支付宝', '银行卡', '信用卡'],
            '商品': ['链接', '货号', '型号', '规格', '尺寸', '颜色', '款式', '品牌', '正品', '假货'],
            '物流': ['发货', '快递', '运费', '到货', '收货', '物流', '包邮', '签收', '退货', '换货']
        }
        
        # 扩展商品词典
        self.product_dict = {
            '品类': ['手机', '电脑', '相机', '耳机', '平板', '手表', '电视', '冰箱', '空调', '洗衣机',
                   '护肤品', '彩妆', '保健品', '零食', '服装', '鞋子', '包包', '饰品', '家具', '母婴用品'],
            '品牌': ['苹果', '华为', '小米', '三星', '索尼', '戴尔', '联想', '海尔', '美的', 'OPPO',
                   '三只松鼠', '耐克', '阿迪达斯', '优衣库', '雅诗兰黛', 'SK-II', '兰蔻', '香奈儿'],
            '属性': ['新款', '限量版', '经典', '高端', '入门', '性价比', '专业', '商务', '轻薄', '时尚',
                   '奢侈', '平价', '国货', '进口', '热销', '爆款', '网红', '明星同款']
        }
        
        # 广告和垃圾信息关键词
        self.spam_keywords = [
            '广告', '推广', '软文', '加我', '私聊', '朋友圈', '免费', '代理', '招商',
            '加盟', '赚钱', '致富', '兼职', '招聘', '有意者', '联系', '微商', '代购'
        ]

    def filter_spam_content(self, content: str) -> bool:
        """过滤垃圾内容
        返回True表示是正常内容，False表示是垃圾内容
        """
        # 检查是否包含垃圾关键词
        for keyword in self.spam_keywords:
            if keyword in content:
                return False
        
        # 检查重复字符
        if len(set(content)) < len(content) * 0.3:  # 如果重复字符过多
            return False
        
        # 检查内容长度
        if len(content) < 2 or len(content) > 500:  # 过滤过短或过长的内容
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

    def get_user_messages(self, wxid: str) -> List[Dict]:
        """获取用户的所有私聊和群聊消息"""
        # 获取私聊消息
        private_messages = list(self.msg_collection.find({
            '$or': [
                {'str_talker': wxid, 'type': {'$in': [1, 3]}},  # 文本消息和图片消息
                {'user_name': wxid, 'type': {'$in': [1, 3]}}  # 群聊中的发言
            ]
        }).sort('create_time', 1))
        
        # 过滤和清理消息
        filtered_messages = []
        for msg in private_messages:
            if msg.get('type') == 1:  # 文本消息
                content = msg.get('str_content', '')
                # 清理文本
                cleaned_content = self.clean_text(content)
                # 过滤垃圾内容
                if cleaned_content and self.filter_spam_content(cleaned_content):
                    msg['str_content'] = cleaned_content
                    filtered_messages.append(msg)
            elif msg.get('type') == 3:  # 图片消息
                filtered_messages.append(msg)
        
        return filtered_messages

    def analyze_shopping_behavior(self, messages: List[Dict]) -> Dict:
        """分析用户的购物行为特征"""
        behavior_stats = {
            'inquiry_count': 0,  # 询价次数
            'purchase_intention_count': 0,  # 购买意向次数
            'payment_count': 0,  # 支付相关次数
            'product_interest': [],  # 感兴趣的商品关键词
            'logistics_count': 0,  # 物流相关次数
            'active_hours': [],  # 活跃时间段
            'last_activity': None  # 最后活动时间
        }
        
        for msg in messages:
            if msg.get('type') == 1:  # 文本消息
                content = msg.get('str_content', '')
                create_time = msg.get('create_time')
                
                # 记录活动时间
                if create_time:
                    behavior_stats['active_hours'].append(create_time.hour)
                    behavior_stats['last_activity'] = create_time
                
                # 分析消息内容
                for category, keywords in self.shopping_keywords.items():
                    for keyword in keywords:
                        if keyword in content:
                            if category == '询价':
                                behavior_stats['inquiry_count'] += 1
                            elif category == '购买意向':
                                behavior_stats['purchase_intention_count'] += 1
                            elif category == '支付':
                                behavior_stats['payment_count'] += 1
                            elif category == '商品':
                                # 提取可能的商品关键词
                                words = jieba.lcut(content)
                                # 过滤停用词
                                words = [w for w in words if w not in self.stopwords]
                                behavior_stats['product_interest'].extend(words)
                            elif category == '物流':
                                behavior_stats['logistics_count'] += 1
        
        # 统计最常见的商品关键词
        if behavior_stats['product_interest']:
            # 过滤掉非商品相关词汇
            product_words = []
            for word in behavior_stats['product_interest']:
                for category, keywords in self.product_dict.items():
                    if word in keywords:
                        product_words.append(word)
            
            counter = Counter(product_words)
            behavior_stats['product_interest'] = counter.most_common(5)
        
        # 统计最活跃的时间段
        if behavior_stats['active_hours']:
            counter = Counter(behavior_stats['active_hours'])
            behavior_stats['active_hours'] = counter.most_common(3)
        
        return behavior_stats

    def generate_user_tags(self, behavior_stats: Dict) -> List[str]:
        """根据行为特征生成用户标签"""
        tags = []
        
        # 购物达人标签
        if behavior_stats['payment_count'] > 5:
            tags.append('购物达人')
        
        # 意向客户标签
        if behavior_stats['inquiry_count'] > 3 or behavior_stats['purchase_intention_count'] > 2:
            tags.append('意向客户')
        
        # 活跃用户标签
        if len(behavior_stats['active_hours']) >= 2:
            tags.append('活跃用户')
        
        # 潜在客户标签
        if behavior_stats['inquiry_count'] > 0 and behavior_stats['payment_count'] == 0:
            tags.append('潜在客户')
        
        # 根据商品兴趣添加标签
        if behavior_stats['product_interest']:
            product_categories = set()
            for word, _ in behavior_stats['product_interest']:
                for category, keywords in self.product_dict.items():
                    if word in keywords:
                        product_categories.add(category)
            
            if '品类' in product_categories:
                tags.append('品类关注者')
            if '品牌' in product_categories:
                tags.append('品牌关注者')
        
        return tags

    def analyze_user_profile(self, wxid: str) -> Dict:
        """分析用户画像"""
        # 获取用户消息
        messages = self.get_user_messages(wxid)
        if not messages:
            return {
                'status': 'error',
                'message': '未找到该用户的消息记录'
            }
        
        # 分析购物行为
        behavior_stats = self.analyze_shopping_behavior(messages)
        
        # 生成用户标签
        tags = self.generate_user_tags(behavior_stats)
        
        # 生成用户画像报告
        report = {
            'status': 'success',
            'wxid': wxid,
            'analysis_time': datetime.now(),
            'message_count': len(messages),
            'behavior_stats': {
                'inquiry_count': behavior_stats['inquiry_count'],
                'purchase_intention_count': behavior_stats['purchase_intention_count'],
                'payment_count': behavior_stats['payment_count'],
                'logistics_count': behavior_stats['logistics_count'],
                'product_interests': behavior_stats['product_interest'],
                'active_time_periods': behavior_stats['active_hours'],
                'last_activity': behavior_stats['last_activity']
            },
            'tags': tags
        }
        
        return report

def analysis(wxid=None):
    # 使用示例
    analyzer = UserProfileAnalyzer(
        mongodb_uri="mongodb://localhost:27017/",
        database_name="test"
    )
    
    # 分析指定用户的画像
    profile = analyzer.analyze_user_profile(wxid)
    
    if profile['status'] == 'success':
        print("\n用户画像分析报告:")
        print(f"微信ID: {profile['wxid']}")
        print(f"分析时间: {profile['analysis_time']}")
        print(f"消息总数: {profile['message_count']}")
        print("\n行为统计:")
        stats = profile['behavior_stats']
        print(f"询价次数: {stats['inquiry_count']}")
        print(f"购买意向次数: {stats['purchase_intention_count']}")
        print(f"支付相关次数: {stats['payment_count']}")
        print(f"物流相关次数: {stats['logistics_count']}")
        if stats['product_interests']:
            print("\n感兴趣的商品关键词:")
            for word, count in stats['product_interests']:
                print(f"  - {word}: {count}次")
        if stats['active_time_periods']:
            print("\n活跃时间段:")
            for hour, count in stats['active_time_periods']:
                print(f"  - {hour}时: {count}次")
        print(f"\n用户标签: {', '.join(profile['tags'])}")
    else:
        print(f"错误: {profile['message']}")

if __name__ == "__main__":
    main()