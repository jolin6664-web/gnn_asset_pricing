# src/data/loader.py
import pandas as pd
import os
from pathlib import Path
from typing import Dict, Tuple

class DataLoader:
    """数据加载器，负责读取和合并原始数据文件"""
    
    def __init__(self, raw_data_path: str = "../data/raw"):
        """
        初始化加载器
        Args:
            raw_data_path: 原始数据文件夹的相对路径
        """
        self.raw_data_path = Path(__file__).parent.parent.parent / raw_data_path
        print(f"数据加载器初始化，数据路径: {self.raw_data_path}")
        
    def load_all(self) -> Dict[str, pd.DataFrame]:
        """
        加载所有原始数据文件
        Returns:
            字典，键为数据集名称，值为DataFrame
        """
        data_dict = {}
        
        # 1. 加载基础信息
        print("正在加载基础信息...")
        basic_info = pd.read_csv(
            self.raw_data_path / "basic_info.txt", 
            sep='\s+', # 匹配空白分隔符
            dtype={'Stkcd': str} # 证券代码保留为字符串，避免丢失前导0
        )
        data_dict['basic_info'] = basic_info
        
        # 2. 加载行业分类
        print("正在加载行业分类...")
        industry_info = pd.read_csv(
            self.raw_data_path / "csrc2012_industry.txt",
            sep='\s+',
            dtype={'Stkcd': str, 'Nnindcd': str}
        )
        data_dict['industry'] = industry_info
        
        # 3. 加载日交易数据
        print("正在加载日交易数据...")
        daily_trade = pd.read_csv(
            self.raw_data_path / "daily_trade.txt",
            sep='\s+',
            dtype={'Stkcd': str},
            parse_dates=['Trddt'] # 解析日期列
        )
        data_dict['daily_trade'] = daily_trade
        
        # 4. 加载月交易数据
        print("正在加载月交易数据...")
        monthly_trade = pd.read_csv(
            self.raw_data_path / "monthly_trade.txt",
            sep='\s+',
            dtype={'Stkcd': str},
            parse_dates=['Trdmnt'] # 注意：这里是年月，如'2023-01'
        )
        data_dict['monthly_trade'] = monthly_trade
        
        # 5. 加载月换手率数据
        print("正在加载月换手率数据...")
        turnover = pd.read_csv(
            self.raw_data_path / "turnover_monthly.txt",
            sep='\s+',
            dtype={'Stkcd': str},
            parse_dates=['Trdmnt']
        )
        data_dict['turnover'] = turnover
        
        print("所有数据加载完成！")
        # 打印各数据集大小预览
        for name, df in data_dict.items():
            print(f"  {name}: {df.shape[0]}行 x {df.shape[1]}列")
            
        return data_dict
    
    def create_merged_monthly_dataset(self, data_dict: Dict[str, pd.DataFrame] = None) -> pd.DataFrame:
        """
        创建合并的月度数据集（用于因子计算的基础表）
        将月度交易数据、换手率、行业信息等合并到一张宽表中
        """
        if data_dict is None:
            data_dict = self.load_all()
            
        print("正在创建合并月度数据集...")
        
        # 获取基础数据
        monthly = data_dict['monthly_trade'].copy()
        turnover = data_dict['turnover'].copy()
        industry = data_dict['industry'].copy()
        basic = data_dict['basic_info'].copy()
        
        # 1. 合并月度交易数据和换手率（关键步骤）
        # 使用证券代码和年月作为合并键
        monthly_merged = pd.merge(
            monthly,
            turnover[['Stkcd', 'Trdmnt', 'ToverOsM']],
            on=['Stkcd', 'Trdmnt'],
            how='left' # 保留所有月度交易记录，即使缺少换手率
        )
        
        # 2. 合并行业信息（每个股票行业信息相对静态，但需注意行业可能变更）
        # 取每个股票最新的行业分类（假设数据中已按时间排序）
        latest_industry = industry.sort_values('Listdt').groupby('Stkcd').last().reset_index()
        monthly_merged = pd.merge(
            monthly_merged,
            latest_industry[['Stkcd', 'Nnindcd', 'Nnindnme']],
            on='Stkcd',
            how='left'
        )
        
        # 3. 合并基础信息（市场类型、上市日期等）
        monthly_merged = pd.merge(
            monthly_merged,
            basic[['Stkcd', 'Markettype', 'Listdt']],
            on='Stkcd',
            how='left'
        )
        
        # 4. 计算一些基础特征
        # 确保数据按股票和日期排序
        monthly_merged = monthly_merged.sort_values(['Stkcd', 'Trdmnt'])
        
        # 计算市值（注意单位转换：原数据是千元）
        monthly_merged['Msmvosd'] = monthly_merged['Msmvosd'] * 1000  # 转换为元
        
        print(f"合并月度数据集创建完成: {monthly_merged.shape[0]}行 x {monthly_merged.shape[1]}列")
        print("包含列:", monthly_merged.columns.tolist())
        
        return monthly_merged

# 简单的测试函数
if __name__ == "__main__":
    # 测试数据加载
    loader = DataLoader()
    data = loader.load_all()
    
    # 测试合并月度数据
    monthly_data = loader.create_merged_monthly_dataset(data)
    
    # 显示前几行
    print("\n合并数据预览:")
    print(monthly_data.head())
    
    # 显示基本信息
    print(f"\n时间范围: {monthly_data['Trdmnt'].min()} 到 {monthly_data['Trdmnt'].max()}")
    print(f"股票数量: {monthly_data['Stkcd'].nunique()}")
    print(f"行业数量: {monthly_data['Nnindcd'].nunique()}")