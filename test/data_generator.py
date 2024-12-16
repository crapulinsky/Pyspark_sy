import csv
import time
import random
from datetime import datetime, timedelta

# 定义数据生成函数
def generate_csv(file_path, num_rows):
    start_date = datetime.strptime("2023-01-01", "%Y-%m-%d")
    end_date = datetime.strptime("2023-12-31", "%Y-%m-%d")

    with open(file_path, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        # 第一行，也是字段名,不要这么多可以根据需求注释掉
        writer.writerow([
             "id",  # 数据唯一标识
            "transaction_date", # 交易日期
            "product_id",   # 产品ID
            "quantity", # 产品数量
            "price",    # 单价
            "customer_id",  # 客户ID
            "store_id", # 店铺ID
            "discount", # 折扣比例
            "total_amount", # 总金额（包括折扣）
            "payment_method",   # 支付方式
            "region", "salesperson_id", # 区域
            "category_id",  # 销售员ID
            "brand",    # 品牌
            "is_returned",  # 是否退货
            "return_reason",    # 退货原因
            "return_date",  # 退货日期
            "delivery_status",  # 交付状态
            "delivery_date",    # 交付日期
            "remarks"   # 备注信息
        ])
        for i in range(1, num_rows + 1):
            transaction_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
            product_id = random.randint(1, 1000)
            quantity = random.randint(1, 100)
            price = round(random.uniform(10.0, 1000.0), 2)
            customer_id = random.randint(1, 50000)
            store_id = random.randint(1, 500)
            discount = round(random.uniform(0.0, 0.5), 2)
            total_amount = round(quantity * price * (1 - discount), 2)
            payment_method = random.choice(["Credit Card", "Cash", "Mobile Payment", "Bank Transfer"])
            region = random.choice(["North", "South", "East", "West"])
            salesperson_id = random.randint(1, 1000)
            category_id = random.randint(1, 50)
            brand = random.choice(["Brand A", "Brand B", "Brand C", "Brand D"])
            is_returned = random.choice([True, False])
            return_reason = random.choice(["Damaged", "Wrong Item", "No Reason", "Other"]) if is_returned else ""
            return_date = (transaction_date + timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d") if is_returned else ""
            delivery_status = random.choice(["Delivered", "Pending", "Cancelled"])
            delivery_date = (transaction_date + timedelta(days=random.randint(1, 10))).strftime("%Y-%m-%d") if delivery_status == "Delivered" else ""
            remarks = random.choice(["", "VIP Customer", "Holiday Sale", "Bulk Order"])

            writer.writerow([
                i,
                transaction_date.strftime("%Y-%m-%d"),
                product_id,
                quantity,
                price,
                customer_id,
                store_id,
                discount,
                total_amount,
                payment_method,
                region,
                salesperson_id,
                category_id,
                brand,
                is_returned,
                return_reason,
                return_date,
                delivery_status,
                delivery_date,
                remarks
            ])


# 参数--------------------------------------------------------
file_path = r"E:\DB\data\test_Random_Data.csv"
num_rows = 1000
#------------------------------------------------------------
if __name__ == '__main__':
    # 开始运行
    start_time = time.time()
    generate_csv(file_path, num_rows)
    # 结束
    end_time = time.time()
    print(f"数据已生成：{file_path}")
    print(f"总行数: {num_rows}")
    # 运行时间
    print("运行时间:", end_time - start_time)
