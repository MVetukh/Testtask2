from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Инициализируем SparkSession
spark = SparkSession.builder.appName("ProductCategoryPairs").getOrCreate()

# Пример данных для DataFrame продуктов
products_df = spark.createDataFrame([
    (1, "Продукт A"),
    (2, "Продукт B"),
    (3, "Продукт C"),
], ["product_id", "product_name"])

# Пример данных для DataFrame связей между продуктами и категориями
product_category_links_df = spark.createDataFrame([
    (1, "Категория 1"),
    (2, "Категория 1"),
    (2, "Категория 2"),
], ["product_id", "category_name"])

# Определим метод для получения пар "Имя продукта – Имя категории" и продуктов без категорий
def get_product_category_pairs(products_df, product_category_links_df):
    # Присоединяем DataFrame'ы по product_id, используя left outer join
    product_category_df = products_df.join(product_category_links_df, "product_id", "left_outer")
    
    # Получаем все пары "Имя продукта – Имя категории"
    product_category_pairs_df = product_category_df.select("product_name", "category_name")
    
    # Отфильтровываем продукты без категорий
    products_without_categories_df = product_category_df.filter(col("category_name").isNull())\
                                                         .select("product_name").distinct()

    return product_category_pairs_df, products_without_categories_df

# Выполнение метода и вывод результатов
product_category_pairs_df, products_without_categories_df = get_product_category_pairs(products_df, product_category_links_df)

print("Все пары «Имя продукта – Имя категории»:")
product_category_pairs_df.show()

print("Имена всех продуктов, у которых нет категорий:")
products_without_categories_df.show()
