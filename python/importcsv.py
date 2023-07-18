import csv
import mysql.connector
import os

# MySQL数据库连接配置
config = {
    'user': 'root',
    'password': '123456',
    'host': '127.0.0.1',
    'port': 3307,
    'auth_plugin': 'mysql_native_password',
    'database': 'factors_ricequant',
    'buffered': True,
}

# 连接到MySQL数据库
connection = mysql.connector.connect(**config)

# 获取数据库游标
cursor = connection.cursor()

# 指定保存CSV文件的路径
save_path = "C:/Users/lenovo/Documents/script"

# 查询所有表格的名称
cursor.execute(f"SHOW TABLES from {config['database']}")
tablenames = [a for (a,) in cursor.fetchall()]
tablenames = ["IND_RANK_RV_IDU", "ORG_RANK", "ORG_RPT_IDU_STAT",
              "ORG_RPT_SEC_CHG", "PERSON_FACT", "SEC_FACT"]

# 遍历所有表格
for table_name in tablenames:
    # 确定CSV文件名（使用表格名称）
    csv_file_name = f"{table_name}.csv"

    # 拼接保存文件的完整路径
    file_path = os.path.join(save_path, csv_file_name)

    # 打开CSV文件以写入数据
    with open(file_path, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)

        # 构造查询表格数据的SQL语句
        query_head = f"SELECT * FROM {table_name} Limit 1 OFFSET 0"
        query = f"SELECT * FROM {table_name}"

        # 写入表头
        cursor.execute(query_head)
        column_names = [i[0] for i in cursor.description]
        csv_writer.writerow(column_names)

        # 分批读取数据并保存到CSV文件
        batch_size = 100000  # 每批次读取的行数
        offset = 0

        while True:
            query_batch = query + f" LIMIT {batch_size} OFFSET {offset}"
            cursor.execute(query_batch)
            rows = cursor.fetchall()

            if not rows:
                break

            csv_writer.writerows(rows)
            offset += batch_size

# 关闭数据库连接
cursor.close()
connection.close()
