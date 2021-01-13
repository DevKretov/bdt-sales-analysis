#!/usr/bin/env python
# coding: utf-8

# The code presented here is tuned for running PySpark on Metacentrum

# Load everything we will need

# In[ ]:





# In[ ]:


import datetime
import os
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from pathlib import Path


# ## Initialise PySpark

# In[ ]:


import pyspark

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext

import pyspark.sql.functions as sf

from pyspark.sql import Window
from pyspark.sql import functions as F

from pyspark.sql.functions import udf
from pyspark.sql.functions import mean as _mean, stddev_pop as _stddev_pop, col


# In[ ]:


sparkSession = SparkSession.builder.appName("bdt-semestral-project").getOrCreate()


# In[ ]:


sparkSession.sql('use kretov')


# In[ ]:


spark_df = sparkSession.sql('select * from sales_log')
spark_df.count()


# In[ ]:


# Delete rows with negative sales
cum_sales_df_filtered = spark_df.filter(spark_df.item_cnt_day > 0)


# In[522]:


output_data_folder = 'output_data'

if not os.path.isdir(output_data_folder):
    os.mkdir(output_data_folder)
    
output_data_plots_path = os.path.join(output_data_folder, 'plots')
if not os.path.isdir(output_data_plots_path):
    os.mkdir(output_data_plots_path)


# ## Reading 2 helper datasets with descriptions
# 
# #### These datasets are stored locally since they are not big at all and there is no need to distribute it
# 
# We will still load them from Hive

# In[493]:


categories_df = sparkSession.sql('select * from item_categories')
items_df = sparkSession.sql('select * from items')


# In[494]:


categories_df.show()


# # ABC/XYZ analysis

# The most important part of the whole assignment is to classify our goods we sell in all our shops along 2 axes:
# 
#     - 1st axis: ABC analysis.
#         We aim to find those goods which contribute to our sales most whilst being a small fraction of the whole assortiment. This is known to be A category. Then, we find less important, but still essential group B and "throw" the rest of goods into category C. 
#         Unfortunately, these categories are vaguely defined and since ABC analysis originates from materials management industry, its meaning in sales is often differently interpreted.
#         
#         As for this assignment, I fix the definition of ABC categories in this way. All the goods are assigned exactly one category basing on the following schema:
#         - We find the number of products sold across different shops and sum this value to produce overall number.
#         - We sort these products according to the number of sold items in a descending order (so that those products which are sold more are pushed to the upper part of the dataframe)
#         - We calculate cumulative number of sold items with the help of sliding windows from up till down of the dataset.
#         - Then, for each row (the number of rows is the same as the number of unique products sold in our shop) we transform this cumulative number of sold items into percentage.
#         - The ABC classification algorithm can take place: A category is assigned to those products having less than 60% of cumulative percentage, B category is assigned to next products under 80% of cumulative percentace, the rest is been assigned the last C category.
#             In the realm of ABC analysis, we expect that the distribution of products in each category will be something like: A - 20% of all products, B - 30% of all products, C - 50% of all products.
#             
#             
#      - 2nd axis: XYZ analysis.
#      
#          The aim of XYZ analysis is to assess the stability of the request for each product. Each uniqie product is assigned a category X, Y or Z (that's the reason for the name of this analysis) depicting the "seasonality" of the sales. That's why it is important to select an appropriate time frame for measuring XYZ classes. 
#          
#          XYZ analysis is defined as assigning a category basing on test statistic known as a coefficient of variation (or relative standard deviation). It's a well-known way of measuring dispersion of a probability distribution or frequency distribution and hence is defined as the ratio of the standard deviation to the mean of the measured values. In our case the mean is been calculated as the average number of sold items of each product for each month. So, it there is data from 2 years of operating our eshop, we will have to calculate the mean from 24 numbers. Then, the standard deviation from the same numbers is been calculated and the final coefficient is been obtained. 
#          
#          Each category is assigned basing on the resulting coefficient of variation:
#              - X is given to those products having values from 0% to 10% (stable requests, no seasonality)
#              - Y is given to those products having values from 10% to 25% (some bigger deviations in sales, possible seasonality)
#              - Z is given to the rest of the products (some extraordinary sales).
#              
#              
# The result of this section is a 2D grid of products that have 2 categories assigned. It will profoundly help us in further analysis.
#            

# ### ABC analysis

# For each item on sale count the number of sold items across all the shops

# In[495]:


cum_sales_df_summed = cum_sales_df_filtered.groupby('item_id').agg(sf.sum('item_cnt_day').alias('item_cnt_day'))


# Sort them in descending order by the number of sold items

# In[496]:


cum_sales_df_summed_sorted = cum_sales_df_summed.sort(cum_sales_df_summed.item_cnt_day.desc())


# Calculate cumulative sum of sold items

# In[497]:


cum_sales_df_cumsum = cum_sales_df_summed_sorted.withColumn('cumsum', 
    F.sum('item_cnt_day').over(Window.orderBy(F.col('item_cnt_day').desc()))
)

cum_sales_df_cumsum.show()

# Take the grand total of the cumulative sum

# In[498]:


cumsum_value = cum_sales_df_cumsum.select(cum_sales_df_cumsum['cumsum']).collect()[-1]
cumsum_value


# Transform the cumulative numbers of sales into the percentages of the grand total value
# 
# This is done in order to define A, B, C classes basing on 60%, 80% and 100% sales thresholds

# In[499]:


round_func = udf(lambda val: round(val / cumsum_value['cumsum'], 3))

cum_sales_df_perc = cum_sales_df_cumsum.withColumn('percentage', round_func(cum_sales_df_cumsum.cumsum))


# Classify each sold item into A, B, C categories

# In[500]:


# Helper function
def abc_classify_func(perc_value):
    # If the cumulative percentage of total sold items is less then 0.6, then it's A category
    if perc_value <= 0.6:
        return 'A'
    # If this item is between 60% and 80% of sold items, then it belongs to B category
    elif perc_value <= 0.8:
        return 'B'
    # Otherwise, it's C
    else:
        return 'C'

# Make this function UDF - user defined function in accordance with Spark documentation
abc_classify = udf(abc_classify_func)

cum_sales_df_abc = cum_sales_df_perc.withColumn('ABC', abc_classify(cum_sales_df_perc.percentage))


# In[501]:


cum_sales_df_abc.show()


# See how many different products have been sold for this timespan

# In[502]:


unique_products_num = cum_sales_df_abc.count()
unique_products_num


# How many items have been sold for this timespan

# In[503]:


cum_sales_df_abc.select('cumsum').collect()[-1]


# Show the percentage of A, B and C categories with respect to the whole dataset

# In[504]:


A_df = cum_sales_df_abc.filter(cum_sales_df_abc['percentage'] <= 0.6)
B_df = cum_sales_df_abc.filter((cum_sales_df_abc['percentage'] <= 0.8) & (cum_sales_df_abc['percentage'] > 0.6))
C_df = cum_sales_df_abc.filter((cum_sales_df_abc['percentage'] <= 1.0) & (cum_sales_df_abc['percentage'] > 0.8))



print(f'The percentage of A products is {(A_df.count()/unique_products_num):.3%}')
print(f'The percentage of B products is {(B_df.count()/unique_products_num):.3%}')
print(f'The percentage of C products is {(C_df.count()/unique_products_num):.3%}')


# ### XYZ analysis

# See if there are negative values

# In[505]:


cum_sales_df_filtered.select('item_cnt_day').describe().show()


# In[506]:


cum_sales_df_filtered.columns


# In[507]:


### Resolve column names differences
if 'date_block_num' in cum_sales_df_filtered.columns:
    cum_sales_df_filtered = cum_sales_df_filtered.withColumnRenamed('date_block_num', 'month_num')


# In[508]:



monthly_sales_df = cum_sales_df_filtered.groupby('month_num', 'item_id').agg(sf.sum('item_cnt_day').alias('item_cnt_month'))


# Calculate mean values and population standard deviation for calculating coefficients of variation

# In[509]:


stats_df = monthly_sales_df.groupby('item_id').agg(
    _mean(col('item_cnt_month')).alias('mean'),
    _stddev_pop(col('item_cnt_month')).alias('std')
)


var_coeff_func = udf(lambda std_val, mean_val: round((std_val / mean_val), 3))

final_stats_df = stats_df.withColumn('var_coeff', var_coeff_func(stats_df.std,stats_df.mean))


# Classify basing on the coefficients of variation values

# In[510]:


def xyz_classify_func(perc_value):
    if perc_value <= 0.10:
        return 'X'
    elif perc_value <= 0.25:
        return 'Y'
    else:
        return 'Z'

xyz_classify = udf(xyz_classify_func)

xyz_classification_df = final_stats_df.withColumn('XYZ', xyz_classify(final_stats_df.var_coeff))


# In[511]:


xyz_classification_df.sort(xyz_classification_df.var_coeff.desc()).show(), xyz_classification_df.count()


# ### ABC/XYZ - putting it all together

# In[512]:


xyz_classification_df_extract = xyz_classification_df.select('item_id', 'XYZ')
cum_sales_df_abc_extract = cum_sales_df_abc.select('item_id', 'ABC')


# Inspect column names of two dataframes so that we will be able to start joining them

# In[513]:


cum_sales_df_abc.columns, xyz_classification_df_extract.columns


# Join both dataframes

# In[514]:


abc_xyz_df = cum_sales_df_abc_extract.join(
    xyz_classification_df_extract,
    cum_sales_df_abc_extract.item_id == xyz_classification_df_extract.item_id,
    'inner'
).\
drop(xyz_classification_df_extract.item_id)

abc_xyz_df.count()


# In[515]:


abc_xyz_df.select('item_id','ABC', 'XYZ').show()


# Save the result

# In[516]:


abc_xyz_df.coalesce(1).write.mode('overwrite').csv(f'{output_data_folder}/abc_xyz_dataframe.csv', header=True)


# ## Visualize ABC/XYZ classification

# In[517]:


abc_xyz_counts = abc_xyz_df.groupby('ABC', 'XYZ').count()
abc_xyz_counts = abc_xyz_counts.sort(abc_xyz_counts.ABC.asc(), abc_xyz_counts.XYZ.asc())


# This dataset is already pretty small containing only 9 rows, so we can proceed with a regular Pandas DataFrame class

# In[518]:


abc_xyz_counts_pd = abc_xyz_counts.toPandas()
print('type of dataframe:')
print(type(abc_xyz_counts_pd))
abc_xyz_counts_pd


# For small extracts or small time spans we expect that there will be no some category (overall there are 9 different assignments of ABC and XYZ categories). In order to display ABC/XYZ, we need to fill in "blank spots".

# In[519]:


# Add missing pairs

pairs = [
    ('A', 'X'),
    ('A', 'Y'),
    ('A', 'Z'),
    ('B', 'X'),
    ('B', 'Y'),
    ('B', 'Z'),
    ('C', 'X'),
    ('C', 'Y'),
    ('C', 'Z'),
]


add_list = []

for pair in pairs:
    if len(abc_xyz_counts_pd[(abc_xyz_counts_pd.ABC == pair[0]) & (abc_xyz_counts_pd.XYZ == pair[1])]) == 0:
        add_list.append([pair[0], pair[1], 0])

# add_list denotes those classes that are needed to be added to the the pandas df so that we have all 9 entries
add_list


# In case when we have some missing category, add them manually so that we are able to create a 3x3 matrix a visualise it

# In[520]:


full_abc_xyz_counts = abc_xyz_counts_pd.append(pd.DataFrame(add_list, columns = abc_xyz_counts_pd.columns))
full_abc_xyz_counts = full_abc_xyz_counts.sort_values(['ABC', 'XYZ'], ascending=[True, True])
full_abc_xyz_counts


# Use Matplotlib to arrange 3x3 categories into a 3x3 grid (matrix) with the number of products falling into each category

# In[523]:


values_matrix = full_abc_xyz_counts['count'].to_numpy().reshape((3, 3))

fig, ax = plt.subplots()

ax.matshow(np.zeros(9).reshape((3, 3)), cmap=plt.cm.Blues)

for i in range(3):
    for j in range(3):
        c = values_matrix[j,i]
        ax.text(i, j, str(c), va='center', ha='center')

        
plt.yticks([0, 1, 2], labels=['A', 'B', 'C'])
plt.xticks([0, 1, 2], labels=['X', 'Y', 'Z'])
ax.xaxis.set_ticks_position('bottom')

plt.title('ABC/XYZ analysis')
plt.savefig(f'{output_data_plots_path}/abc_xyz_grid.png')
plt.show()


# As we can see from the matrix above we have 9 products from category A which represent the most vital part of the presented products in our business since A category says that this product falls into the bucket of goods being sold most of the time, and X category denoting strong stability of the need for these products. That's why it is crucial to know which products are these. 
# 
# Then, we see that most of our goods are classified with Z label saying that we have lots products being affected by some factors (social and economic, maybe cultural as well) causing the request for these products less stable and more volatile. We will examine it later.
# 
# We also have to take into consideration the goods from BX category, since they produce less number of sold items but they are believed to be stable enough, so that we can sell them all seasons and holidays regardless.

# In[524]:


# for pair in pairs:
#     print(f'\n\nCategory {pair[0]:2}{pair[1]:2} representatives:\n')
    
#     pair_df = abc_xyz_df.filter((abc_xyz_df.ABC == pair[0]) & (abc_xyz_df.XYZ == pair[1])).limit(10)
    
#     pair_df_with_names = pair_df.join(items_df, pair_df.item_id == items_df.item_id).drop(items_df.item_id)
#     products = pair_df_with_names.select('item_name').collect()
    
#     for product in products:
#         print(product.__getitem__('item_name'))
    


# What can we say here? We see the clear trend that console games are being sold really solidly and in a stable fashion. Some games are less popular than others, but still we see lots of known representatives of game industry popular in the presented years. These years are also characterized as the rise of console gaming, which we see reflected in our data: PC games are less stable (judging by top 10 values of the list presented here, we will study this effect more thoroughly in next sections).

# # Exploratory analysis

# ### Average product price

# In[525]:


mean_prices_df = cum_sales_df_filtered.groupby('item_id').agg(sf.mean('item_price').alias('mean_price'))
mean_prices_df = mean_prices_df.join(items_df, mean_prices_df.item_id == items_df.item_id, 'inner').drop(items_df.item_id)
mean_prices_df = mean_prices_df.sort(mean_prices_df.mean_price.desc())

top_10_df = mean_prices_df.take(10)

for row in top_10_df:
    mean_price = round(row['mean_price'], 2)
    print(row['item_name'], mean_price)


# In[526]:

print('type of dataframe:')
print(type(mean_prices_df))

mean_prices_pandas_df = mean_prices_df.toPandas()
print('type of dataframe:')
print(type(mean_prices_pandas_df))
#mean_prices_pandas_df = mean_prices_pandas_df.collect()


# In[527]:


mean_prices_pandas_df.to_csv(os.path.join(output_data_folder, 'mean_product_prices_sorted.csv'))


# Before drawing plots and showing tables, we will do some basic robust statistics and truncate our dataset by the value of quantile 0.95 in terms of mean prices.
# 
# In the end, we will show both data frames (cleaned from the outliers and the whole one) to see the difference

# In[528]:


quantile_095 = mean_prices_pandas_df['mean_price'].quantile(0.95)

hist_data = mean_prices_pandas_df[mean_prices_pandas_df['mean_price'] <= quantile_095]
hist_data


# In[529]:


hist_data.mean_price.describe()


# In[531]:


fig = plt.figure(figsize=(20,8))
sns.displot(
    data=hist_data,
    x='mean_price',
    binwidth=100
)
plt.title('Histogram of products mean prices')
plt.xlabel('Mean prices of products in rubles, up to 0.95 quantile')
plt.ylabel('Number of products')
plt.savefig(os.path.join(output_data_plots_path, 'mean_prices_products_histogram.png'))
plt.show()


# Histogram with outliers

# In[532]:


fig = plt.figure(figsize=(20,8))
sns.displot(
    data=mean_prices_pandas_df,
    x='mean_price',
   # binwidth=100
)
plt.title('Histogram of products mean prices')
plt.xlabel('Mean prices of products in rubles, up to 0.95 quantile')
plt.ylabel('Number of products')
plt.show()


# Well, the decision to get rid of outliers was a correct decision, let's see who are they

# In[533]:


mean_prices_pandas_df[mean_prices_pandas_df['mean_price'] > quantile_095].head(20)


# The outliers are some big SW packages and huge console sets that are not sold in big quantities

# ### Categories average

# In order to process data about categories, we also have to join our logs with categories information

# In[539]:


join_1 = cum_sales_df_filtered.join(
    items_df,
    cum_sales_df_filtered.item_id == items_df.item_id,
    'inner'
).drop(items_df.item_id)

final_join = join_1.join(
    categories_df,
    categories_df.item_category_id == join_1.item_category_id,
    'inner'
).drop(join_1.item_category_id)


# In[540]:


categories_sum = final_join.groupby(
    'item_category_id', 'item_category_name'
).agg(
    sf.sum('item_cnt_day').alias('sales_sum')
)

categories_sum = categories_sum.sort(categories_sum.sales_sum.desc())


# In[541]:


top_5_categories_df = categories_sum.limit(5).toPandas()#.collect()
top_5_categories = top_5_categories_df.item_category_id.tolist()

top_5_categories_df


# In[542]:


top_5_categories_sales_df = final_join.filter(final_join.item_category_id.isin(top_5_categories))

top_5_categories_sales_df = top_5_categories_sales_df.select(
    top_5_categories_sales_df.item_id,
    top_5_categories_sales_df.item_category_id,
    top_5_categories_sales_df.item_category_name,
    top_5_categories_sales_df.item_price,
    top_5_categories_sales_df.item_name
)


# In[543]:


cat_mean_prices_df = top_5_categories_sales_df.groupby(
    'item_id', 'item_category_id', 'item_category_name', 'item_name'
).agg(
    sf.mean('item_price').alias('mean_price')
)

cat_mean_prices_df = cat_mean_prices_df.sort(cat_mean_prices_df.mean_price.desc())
#cat_mean_top_10 = cat_mean_prices_df.take(10)

mean_cat_prices_pandas_df = cat_mean_prices_df.toPandas()#.collect()

mean_cat_prices_pandas_df.to_csv(os.path.join(output_data_folder, 'mean_category_prices_top_5.csv'))


# Since we have several aggregate categories, we cannot apply the same logic for outliers filtering as we did in mean product prices. That's why we clean the data frame step by step for each category and then construct the new one to plot

# In[544]:


hist_df_list = []

for category in top_5_categories:
    cat_extract = mean_cat_prices_pandas_df[mean_cat_prices_pandas_df['item_category_id'] == category]
    quantile = cat_extract.mean_price.quantile(0.95)
    cat_extract = cat_extract[cat_extract.mean_price <= quantile]
    
    hist_df_list.append(cat_extract)
    
hist_df = pd.concat(hist_df_list)


# In[545]:


hist_df


# This is irrelevant statistic telling nothing about our data

# In[546]:


#hist_df.mean_price.describe()


# This is how it has to be done for each category

# In[547]:


cat_index = 0

hist_df_list[cat_index].mean_price.describe()


# In[548]:


g = sns.displot(
    hist_df,
    x='mean_price',
    col='item_category_name',
    kind='kde',
    col_wrap=2,
    fill=True,
    facet_kws={
        'sharey':False,
        'sharex':False
    }
)

g.set_axis_labels("Average price in rubles ", "Density")
g.set_titles(col_template="{col_name}")

plt.savefig(os.path.join(output_data_plots_path, 'mean_prices_categories_kde_histogram.png'))


# The graphs above show some kind of stability in mean prices in games categories (PC games and PS3 games), whereas movies categories have "heavier tails" of their distributions. It's expected that Blu-Ray films have average prices a little bit higher than DVDs since they are considered to be of better quality. CD disks with music have to distinct peaks and two interesting tails, which may be understood as a mixture of two distributions. A more thorough analysis has to be done in order to understand the underlying factors of such a distribution.

# ### Average category price

# In[549]:


categories_prices = final_join.select('item_category_id', 'item_category_name', 'item_price')


# In[550]:


categories_mean_prices = categories_prices.groupby(
    'item_category_id', 'item_category_name'
).agg(
    sf.mean('item_price').alias('mean_price')
)
categories_mean_prices = categories_mean_prices.sort(categories_mean_prices.mean_price.desc())


# In[551]:


categories_mean_prices_pd_df = categories_mean_prices.toPandas()#.collect()

categories_mean_prices_pd_df.to_csv(os.path.join(output_data_folder, 'mean_price_for_category.csv'))


# In[552]:


#plt.hist(hist_data.mean_price, density=False, bins=30)  # density=False would make counts
sns.displot(
    data=categories_mean_prices_pd_df,
    x='mean_price',
    binwidth=200
)
plt.title('Histogram of categories average prices')
plt.xlabel('Average categories prices in rubles')
plt.ylabel('Number of categories')
plt.savefig(os.path.join(output_data_plots_path, 'mean_category_price_histogram.png'))
plt.show()


# What do we see here? Our categories are a little bit unbalanced in terms of mean prices of the products that are sold in them. While most of our products build the left side of the histogram, there are some categories having much higher price range. Let's examine which categories are in the right part of the picture.

# In[553]:


cat_mean_prices_top_10 = categories_mean_prices.limit(10)
cat_mean_prices_top_10 = cat_mean_prices_top_10.toPandas()#.collect()


# In[554]:


cat_mean_prices_top_10


# Due to the fact that our shops mostly sell disks with PC and console games and other stuff like films, DVDs and CD disks with music and since these items account for the most of the whole assortiment, most of categories' average prices are below 2000 Russian rubles. However, not only do we sell disks, but also some consoles and software which cost much more than an average CD disk. That's the reason for having several categories with much higher average price range.

# ## Top 10 overall sales products for AX, BX, CZ categories

# First of all, we have to combine two data frames with XYZ and ABC classifications

# In[555]:


xyz_classification_df.show()


# In[556]:


cum_sales_df_abc.show()


# Join these data frames into one big data frame

# In[557]:


cum_sales_big_df = xyz_classification_df.join(
    cum_sales_df_abc,
    xyz_classification_df.item_id == cum_sales_df_abc.item_id,
    'inner'
).drop(cum_sales_df_abc.item_id)


# Calculate total sale for each product

# In[558]:


cum_sales_big_df_total_sale = cum_sales_df_filtered.withColumn('total_sale', cum_sales_df_filtered.item_price * cum_sales_df_filtered.item_cnt_day)


# In[559]:


total_sale_df = cum_sales_big_df_total_sale.groupby('item_id').agg(sf.sum('total_sale').alias('total_sale'))


# Join this data to create ABC/XYZ classes

# In[560]:


final_df_joined = cum_sales_big_df.join(
    total_sale_df,
    cum_sales_big_df.item_id == total_sale_df.item_id,
    'inner'
).drop(total_sale_df.item_id)


# ## Categories

# Here the process in pretty straightforward and identical for all three categories:
# 
# 1. Filter out products belonging to the category of interest
# 2. Select only relevant columns
# 3. Join the resulting data frame with items dataframe so that we have products' readable names
# 4. Sort the final data frame by final sales column in a descending order so that we select top 10 products with the highest sales
# 5. Convert these 10 rows into Pandas dataframe, save it as a CSV report file and show here in the notebook

# ### AX category

# In[561]:


ax_final_sales = final_df_joined.filter(
    (final_df_joined.ABC == 'A') & (final_df_joined.XYZ == 'X')
)

ax_final_sales = ax_final_sales.select('item_id', 'abc', 'xyz', 'total_sale')
ax_final_sales_with_names = ax_final_sales.join(items_df, ax_final_sales.item_id == items_df.item_id).drop(items_df.item_id)

ax_final_sales_top_10 = ax_final_sales_with_names.sort(ax_final_sales_with_names.total_sale.desc()).limit(10)


# In[562]:


ax_final_sales_top_10 = ax_final_sales_top_10.toPandas()#.collect()
ax_final_sales_top_10.to_csv(os.path.join(output_data_folder, 'top_10_products_by_sale_in_ax.csv'))


# In[563]:


ax_final_sales_top_10


# The most stable goods accounting for the most profitable sales are console and PC games by popular vendors. We also see that our shop is regularly visited by people with great music preferences since they buy the film about Pink Floyd  :)

# ### BX category

# In[564]:


bx_final_sales = final_df_joined.filter(
    (final_df_joined.ABC == 'B') & (final_df_joined.XYZ == 'X')
)

bx_final_sales = bx_final_sales.select('item_id', 'abc', 'xyz', 'total_sale')
bx_final_sales_with_names = bx_final_sales.join(items_df, bx_final_sales.item_id == items_df.item_id).drop(items_df.item_id)

bx_final_sales_top_10 = bx_final_sales_with_names.sort(bx_final_sales_with_names.total_sale.desc()).limit(10)


# In[565]:


bx_final_sales_top_10_pd = bx_final_sales_top_10.toPandas()#.collect()
bx_final_sales_top_10_pd.to_csv(os.path.join(output_data_folder, 'top_10_products_by_sale_in_bx.csv'))


# In[566]:


bx_final_sales_top_10_pd


# Here we already see not only some games for PC and PS/XBOX, but also for more budget consoles like PS Vita, we also see the representatives of PS4 accessories. It means that these items are less stable in our sales, but still considerably constribute to our shop's wellbeing.

# ### CZ category

# In[567]:


cz_final_sales = final_df_joined.filter(
    (final_df_joined.ABC == 'C') & (final_df_joined.XYZ == 'X')
)

cz_final_sales = cz_final_sales.select('item_id', 'abc', 'xyz', 'total_sale')
cz_final_sales_with_names = cz_final_sales.join(items_df, cz_final_sales.item_id == items_df.item_id).drop(items_df.item_id)

cz_final_sales_top_10 = cz_final_sales_with_names.sort(cz_final_sales_with_names.total_sale.desc()).limit(10)


# In[568]:


cz_final_sales_top_10_pd = cz_final_sales_top_10.toPandas()#.collect()
cz_final_sales_top_10_pd.to_csv(os.path.join(output_data_folder, 'top_10_products_by_sale_in_cz.csv'))


# In[569]:


cz_final_sales_top_10_pd


# There are some console games here as well, but I cannot say that they are somehow popular. We also see more sets here (Xbox One set) and (Sony PS Vita set), which proves that these sales are less frequent. 

# ## Total sales w.r.t days, weeks, months

# In[570]:


### Resolve column names differences
if 'date' in cum_sales_big_df_total_sale.columns:
    cum_sales_big_df_total_sale = cum_sales_big_df_total_sale.withColumnRenamed('date', 'sale_date')


# First of all, we will calculate total sale with respect to each day present in the report.
# We also preserve month_num column since it will help us orient in different months later

# In[571]:


total_price_to_date_df = cum_sales_big_df_total_sale.groupby('sale_date', 'month_num').agg(
    sf.sum('total_sale').alias('total_daily_sale')
)

number_of_days_in_the_dataset = total_price_to_date_df.count()

print(f'Days logged in the dataset: {number_of_days_in_the_dataset}')


# We have not so many rows, so let's convert this data frame into Pandas data frame

# In[572]:


total_price_to_date_pandas_df = total_price_to_date_df.toPandas()#.collect()


# We will greately benefit from Pandas' DateTime data type since it provides lots of methods of converting date and time and also perform "datetime" arithmetics, making the whole analytical process much easier

# In[573]:


total_price_to_date_pandas_df.dtypes


# One more advantage of working with DateTime columns is the ability to sort these columns in hierarchical order

# In[574]:


total_price_to_date_pandas_df = total_price_to_date_pandas_df.sort_values(by=['sale_date'], ascending=True,).reset_index(drop=True)

total_price_to_date_pandas_df.to_csv(os.path.join(output_data_folder, 'total_price_to_date.csv'))


# In[575]:


total_price_to_date_pandas_df


# Our next task is to compute the week number since we also want to aggregate sales information with respect to weeks.
# 
# The algorithm is almost trivial: take the very first date available in the data frame, determine the day of the week from the date and subtract this date by the ordinal number of this day w.r.t. the week. By doing so we will get the date of the closest Monday preceeding this particular date. This Monday will be the "baseline" date.

# In[576]:


total_price_to_date_pandas_df['sale_date'] = pd.to_datetime(total_price_to_date_pandas_df['sale_date'])

date_1 = total_price_to_date_pandas_df['sale_date'][0]
monday_start = date_1 -  pd.Timedelta(days = date_1.weekday())

print(f'The first date in our sales log is {date_1}')
print(f'The closest Monday preceeding this day is on {monday_start}')


# And now with the help of simple calculations we obtain the week number for each entry in the sales data frame

# In[577]:


def get_relative_week_num(date_2, start_monday_date):
    monday_2 = date_2 - pd.Timedelta(days = date_2.weekday())
    diff = (monday_2 - start_monday_date).days
    
    return diff // 7

pd_week_func = lambda date: get_relative_week_num(date, monday_start)
    


# Add new column "week_num" and show the resulting data frame

# In[578]:


total_price_to_date_pandas_df['week_num'] = total_price_to_date_pandas_df['sale_date'].apply(pd_week_func)

total_price_to_date_pandas_df


# The last step is to get the relative day number. It's even more trivial than with the weeks

# In[579]:


pd_relative_days_func = lambda date: (date - date_1).days


# In[580]:


total_price_to_date_pandas_df['day_num'] = total_price_to_date_pandas_df['sale_date'].apply(pd_relative_days_func)

total_price_to_date_pandas_df


# Change columns order for more logical view

# In[581]:


total_price_to_date_pandas_df = total_price_to_date_pandas_df[total_price_to_date_pandas_df.columns[[0, -1, -2, 1, 2]]]
total_price_to_date_pandas_df


# In order to get fancier visualisations, we will make use of some formatters and tickers in matplotlib library

# In[582]:


import matplotlib.dates as mdates
import matplotlib.ticker as ticker

day_locator = mdates.DayLocator((1,10, 20,))
week_locator = ticker.MultipleLocator(10)
month_locator = mdates.MonthLocator()

formatter_func = lambda x, pos: f'{x/10**6:,.0f}M'
formatter = ticker.FuncFormatter(formatter_func)


# In[583]:


fig = plt.figure(figsize=(20,10))

ax = sns.lineplot(
    data=total_price_to_date_pandas_df,
    x='sale_date',
    y='total_daily_sale'
)
ax.xaxis.set_major_locator(day_locator)
ax.yaxis.set_major_formatter(formatter)

plt.xticks(rotation=90)
plt.grid()
plt.title('Total sales graph per each day')
plt.xlabel('Date')
plt.ylabel('Sales in rubles')
plt.savefig(os.path.join(output_data_plots_path, 'sales_per_each_day.png'))
plt.show()


# By looking at this graph we can see that there are several peaks. The most distinct one is near the end of 2013 and might be explained by the approaching New Year's holidays. Lots of people buy presents, so the reason for such a rise in sales can be just a reflection of this fact.
# 
# There are also some smaller peaks near the end of February and the beginning of March. There are also 2 major holidays for men (23.02) and women (08.03), this may play some role in such fluctuations.
# 
# There is even bigger peak just at the end of spring and the beginning of summer. I can explain it by possible higher correlation of number of console and PC games sold and the fact that children and youngsters finish their studies. I'm almost 100% sure there is positive and strong correlation between these two variables. That's wht the number of sales rose for a short period of time.
# 
# Some peaks in the middle of the autumn are not connected to any particular and important holiday in Russia. Then, it is probably due to some discounts or final sales before buying newer goods/games before the next New Year hot season. It might also be due to some major events in game or movie industry (the unveiling of new PlayStation or new XBox with some new exlusive games in parallel).
# 
# By the end of the next year we also see the rising trend of sales since there are new holidays in a stone's throw and lots of people again have to buy presents.

# #### Weekly data visualisation

# In[584]:


weekly_sales_df = total_price_to_date_pandas_df[['sale_date', 'week_num', 'total_daily_sale']].groupby(
    'week_num'
).agg(
{'sale_date': np.min, 'total_daily_sale': np.sum}
).rename(
    columns=
    {
        'sale_date': 'week_start_date',
        'total_daily_sale': 'weekly_sale'
    }
)

weekly_sales_df


# In[585]:


fig = plt.figure(figsize=(20,10))

ax = sns.lineplot(
    data=weekly_sales_df,
    x='week_start_date',
    y='weekly_sale',
    marker = 'o'
)
ax.xaxis.set_major_locator(week_locator);
ax.yaxis.set_major_formatter(formatter)

plt.xticks(rotation=90)
plt.grid()
plt.title('Total sales graph per each week')
plt.xlabel('Date')
plt.ylabel('Sales in rubles')
plt.savefig(os.path.join(output_data_plots_path, 'sales_per_each_weak.png'))
plt.show()


# In my opinion, this graph is even more informative than the previous one since one day is not particularly good time span for seeing some tendencies. A weekly span has an advantage of encompassing weekends and the "mood" for the current week. Obviously, the "mood" of the week before the New Year's holidays and the "mood" the very first week after them are too different and we see it in the graph as well. Weeks are less prone to radical fluctuations and show more general trend, whereas day-to-day data show more variety which is mostly preserved in weekly data and is easier to look at.
# 
# By looking at this graph, we see that two most considerable peaks are drawn tighlty before the New Year's holidays. These peaks are clearer than those in the preceeding graph. The peak at the beginning of summer is also depicted here, which once more shows some evidence of strong factors playing its role on total sales in our shop.

# #### Monthly sales visualisation

# In[586]:


monthly_sales_df = total_price_to_date_pandas_df[['sale_date', 'month_num', 'total_daily_sale']].groupby(
    'month_num'
).agg(
{'sale_date': np.min, 'total_daily_sale': np.sum}
).rename(
    columns=
    {
        'sale_date': 'month_start_date',
        'total_daily_sale': 'monthly_sale'
    }
)

monthly_sales_df


# In[587]:


fig = plt.figure(figsize=(20,10))

ax = sns.lineplot(
    data=monthly_sales_df,
    x='month_start_date',
    y='monthly_sale',
    marker = 'o'
)
ax.xaxis.set_major_locator(month_locator)
ax.yaxis.set_major_formatter(formatter)

plt.xticks(rotation=90)
plt.grid()
plt.title('Total sales graph per each month')
plt.xlabel('Month start date')
plt.ylabel('Sales in rubles')
plt.savefig(os.path.join(output_data_plots_path, 'sales_per_each_month.png'))
plt.show()


# In fact, nothing really suprising. Again, two peaks at the end of each year, slightly more sales at the end of winters and a slight rise in sales at the beginning of summer holidays. 
# 
# Moreover, we see some gradual and increasing demand after the beginning of Octobers. It may signalize that many people start buying presents in advance, so the message for our supply department may be pretty straightforward: we have to get ready to the higher demands and "...wake up when September ends" (c) Green Day.

# ### Day of week sales, day of month sales, week in year sales, month in year sales

# #### Day of the week

# In[588]:


pd_day_of_the_week_func = lambda date: date.dayofweek

total_price_to_date_pandas_df['day_of_the_week'] = total_price_to_date_pandas_df['sale_date'].apply(pd_day_of_the_week_func)
total_price_to_date_pandas_df


# In[589]:


weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']


# In[590]:


plt.figure(figsize=(15, 10))

agg_df = total_price_to_date_pandas_df[
    ['day_of_the_week', 'total_daily_sale']
].groupby('day_of_the_week').sum()

plt.bar(agg_df.index, agg_df['total_daily_sale'])
plt.title('Sales distribution along the day of the week')
plt.xlabel('Day of week')
plt.xticks(list(range(len(weekdays))), labels = weekdays)
plt.ylabel('Total sale')
plt.savefig(os.path.join(output_data_plots_path, 'sales_per_each_day_of_the_week.png'))
plt.show()


# Nothing really surprising here. We sell more goods over the weekend, since kinds and youngsters mostly do not buy games for themselves and employ their parents who mostly work from Monday to Friday and have more time during the weekends. Here we can clearly see evidence for my hypothesis. 

# #### Day of the month

# In[591]:


pd_day_of_the_month_func = lambda date: date.day

total_price_to_date_pandas_df['day_of_the_month'] = total_price_to_date_pandas_df['sale_date'].apply(pd_day_of_the_month_func)
total_price_to_date_pandas_df


# In[592]:


plt.figure(figsize=(15, 10))

agg_df = total_price_to_date_pandas_df[
    ['day_of_the_month', 'total_daily_sale']
].groupby('day_of_the_month').sum()

plt.bar(agg_df.index, agg_df['total_daily_sale'])
plt.title('Sales distribution over the day of the month')
plt.xlabel('Day of the month')
plt.xticks(agg_df.index)
plt.ylabel('Total sale')
plt.savefig(os.path.join(output_data_plots_path, 'sales_per_each_day_of_the_month.png'))
plt.show()


# I will steer clear from any possible hypotheses concerning this plot... I can only say that I see four distinct "waves" in sales. They maybe have something to do with the fact that there are 4 weeks in each month, but still they are not alligned to some particular date (a month can start on Friday or on any other day of the week, so the data presented here might be misleading). Since we are looking on data from 2 years of selling, it's highly probable that were quite lucky with the allignment.
# 
# The only thing that may be relevant to real life after pondering about these results is the fact that most people get paid several days before the end of each month, so they may be less reluctant to buy a new video game or music disk, that's why they may spend more spare money right after getting it.

# #### Week of the year

# In[593]:


pd_week_of_the_year_func = lambda date: date.weekofyear

total_price_to_date_pandas_df['week_of_the_year'] = total_price_to_date_pandas_df['sale_date'].apply(pd_week_of_the_year_func)
total_price_to_date_pandas_df


# In[594]:


plt.figure(figsize=(15, 10))

agg_df = total_price_to_date_pandas_df[
    ['week_of_the_year', 'total_daily_sale']
].groupby('week_of_the_year').sum()

plt.bar(agg_df.index, agg_df['total_daily_sale'])
plt.title('Sales distribution over the weeks of the year')
plt.xlabel('Week of the year')
plt.xticks(agg_df.index)
plt.ylabel('Total sale')
plt.savefig(os.path.join(output_data_plots_path, 'sales_per_each_week_of_the_year.png'))
plt.show()


# This graph is a something of a merge of several years presented above in weekly data. We get the idea of how the year looks like by summing up several years, which may produce a more balanced picture of how our sales are distributed throughout the year. 
# 
# The conclusion that can be drawn is pretty similar to the one presented under the weekly sales plot. We see that most sales are achieved at the end of the year with some momentum at the beginning of the next one. Then, we see some rising trend at the beginning of spring and summer. 
# 
# In other words, this graphs summarises what we've seen earlier, and significantly improves our visual abilities as soon as we want to assess more than 2-3 years (in such a case, the weekly graph above might become unreadable and cluttered).

# #### Month of the year

# In[595]:


pd_month_of_the_year_func = lambda date: date.month

total_price_to_date_pandas_df['month_of_the_year'] = total_price_to_date_pandas_df['sale_date'].apply(pd_month_of_the_year_func)
total_price_to_date_pandas_df


# In[596]:


plt.figure(figsize=(15, 10))

agg_df = total_price_to_date_pandas_df[
    ['month_of_the_year', 'total_daily_sale']
].groupby('month_of_the_year').sum()

plt.bar(agg_df.index, agg_df['total_daily_sale'])
plt.title('Sales distribution over the months of the year')
plt.xlabel('Month of the year')
plt.xticks(agg_df.index)
plt.ylabel('Total sale')
plt.savefig(os.path.join(output_data_plots_path, 'sales_per_each_month_of_the_year.png'))
plt.show()


# Well, it's one more way how to comfortably and informatively summarize many years in one and present balanced view on the data. We again notice higher demand closer to the end of the year and slowly rising demand till the beginning of the spring (some people buy presents 1-2 months in advance) and even more slowly increasing demand till the 6th month of the year (it's June). 
# 
# The 9th months is surprisingly higher in demands compared to both 8th and 10th months. I can hypothesize that there are two possible reasons causing this effect:
# 
# 1. Children go to school and hence, parents buy them something for school or something to relief their stress.
# 2. Lots of people are being born in Russia in September (well, we might guess why, but I'm sure that it is due to 10 days of doing nothing during New Year holidays which may result in conveiving a child after impetuous celebrations of both men and women).

# ## The same as before but just in 5 biggest categories

# In[597]:


top_5_categories_ids_list = top_5_categories_sales_df.toPandas().item_category_id.unique()
top_5_categories_ids_list


# In[598]:


_join = cum_sales_big_df_total_sale.join(
    items_df,
    cum_sales_df_filtered.item_id == items_df.item_id,
    'inner'
).drop(items_df.item_id)

cum_sales_big_df_total_sale_categories = _join.join(
    categories_df,
    categories_df.item_category_id == _join.item_category_id,
    'inner'
).drop(_join.item_category_id)


# In[599]:


total_price_to_date_df = cum_sales_big_df_total_sale_categories.filter(
    cum_sales_big_df_total_sale_categories.item_category_id.isin(
        top_5_categories_ids_list.tolist()
    )
).groupby(
    'sale_date',
    'month_num',
    'item_category_id',
    'item_category_name',
).agg(
    sf.sum(
        'total_sale'
    ).alias(
        'total_daily_sale'
    )
)

total_price_to_date_df.count()


# In[600]:


total_price_to_date_pd_df = total_price_to_date_df.toPandas()#.collect()


# In[601]:


total_price_to_date_pd_df['sale_date'] = pd.to_datetime(total_price_to_date_pd_df['sale_date'])

print(total_price_to_date_pd_df)
print(total_price_to_date_pd_df.dtypes)


total_price_to_date_pd_df = total_price_to_date_pd_df.sort_values(by=['sale_date'], ascending=True,).reset_index(drop=True)
total_price_to_date_pd_df


# The same process as before, just reusing this code

# In[603]:



date_1 = total_price_to_date_pd_df['sale_date'][0]
monday_start = date_1 -  pd.Timedelta(days = date_1.weekday())

date_1, monday_start


# In[604]:


total_price_to_date_pd_df['week_num'] = total_price_to_date_pd_df['sale_date'].apply(pd_week_func)


# In[605]:


total_price_to_date_pd_df['day_num'] = total_price_to_date_pd_df['sale_date'].apply(pd_relative_days_func)


# In[606]:


total_price_to_date_pd_df['day_of_the_week'] = total_price_to_date_pd_df['sale_date'].apply(pd_day_of_the_week_func)


# In[607]:


total_price_to_date_pd_df['day_of_the_month'] = total_price_to_date_pd_df['sale_date'].apply(pd_day_of_the_month_func)


# In[608]:


total_price_to_date_pd_df['week_of_the_year'] = total_price_to_date_pd_df['sale_date'].apply(pd_week_of_the_year_func)


# In[609]:


total_price_to_date_pd_df['month_of_the_year'] = total_price_to_date_pd_df['sale_date'].apply(pd_month_of_the_year_func)


# In[610]:


total_price_to_date_pd_df


# #### By weekday

# In[611]:


agg_df = total_price_to_date_pd_df[
    ['item_category_name', 'day_of_the_week', 'total_daily_sale']
].groupby(['item_category_name', 'day_of_the_week']).sum()


# In[612]:


plt.figure(figsize=(10,8))
ax = sns.lineplot(
    data=agg_df,
    x='day_of_the_week',
    y='total_daily_sale',
    hue='item_category_name',
    palette='twilight',
    marker='o'
)
ax.yaxis.set_major_formatter(formatter)
plt.title('Sales by biggest 5 categories per day of a week')
plt.xlabel('Day of week')
plt.xticks(list(range(len(weekdays))), labels = weekdays)
plt.ylabel('Sales in rubles')
plt.grid()
plt.savefig(os.path.join(output_data_plots_path, 'top_5_categories_sales_per_each_day_of_the_week.png'))
plt.show()


# Again, we are not surprised: our products have lots in common and thus the sales depict it pretty clearly. The sales in top 5 categories have the strongest effect on data we saw in previous graphs, so we anticipate the same pattern here.
# 
# However, we see that PS3 games are more dominant than PC games and XBox games. It's true since PS3 itself is more popular than Microsoft's console (even though I have XBox 360 at home :( )

# ### By a day of the month

# In[613]:


agg_df = total_price_to_date_pd_df[
    ['item_category_name', 'day_of_the_month', 'total_daily_sale']
].groupby(['item_category_name', 'day_of_the_month']).sum()


# In[614]:


plt.figure(figsize=(20,10))
ax = sns.lineplot(
    data=agg_df,
    x='day_of_the_month',
    y='total_daily_sale',
    hue='item_category_name',
    palette='twilight',
    marker='o'
)
ax.yaxis.set_major_formatter(formatter)
plt.title('Sales by biggest 5 categories per day of a month')
plt.xlabel('Day of month')
plt.ylabel('Sales in rubles')
plt.grid()
plt.savefig(os.path.join(output_data_plots_path, 'top_5_categories_sales_per_each_day_of_a_month.png'))
plt.show()


# Here we see that PS3 games show some bizarre behaviour which is less correlated with other 4 categories. While other 4 categories have lots in common (if we considered these observations in terms of differentiating our dataset into distinct groups, our hypothesis will fail since DVDs and Music categories show almost identical trend, Blu-Ray disks have the same slope (their 1st derivative will be as well pretty similar to DVDs and music CDs) despite being a little bit higher (well, it's common knowledge that Blu-Rays are much more expensive than regular DVDs). PC games, on the other hand, show different behaviour than PS3 games. The reason for such deviation may lie in the fact that starting from the rise of PS3 and XBox, game industry has split into two almost separate and non-overlapping areas with PS3/XBox having more exlusive content and being more budget-friendly compared to PCs of comparative power.

# ### By a week of the year

# In[615]:


agg_df = total_price_to_date_pd_df[
    ['item_category_name', 'week_of_the_year', 'total_daily_sale']
].groupby(['item_category_name', 'week_of_the_year']).sum()


# In[616]:


plt.figure(figsize=(20,10))
ax = sns.lineplot(
    data=agg_df,
    x='week_of_the_year',
    y='total_daily_sale',
    hue='item_category_name',
    palette='twilight',
    marker='o'
)
ax.yaxis.set_major_formatter(formatter)
plt.title('Sales by biggest 5 categories per week of a year')
plt.xlabel('Week of the year')
plt.ylabel('Sales in rubles')
plt.grid()
plt.savefig(os.path.join(output_data_plots_path, 'top_5_categories_sales_per_each_day_of_a_year.png'))
plt.show()


# Everything is pretty clear for DVDs, CDs and Blu-Rays. No need to discuss it.
# 
# What's more eye-catching are several peaks for both PS3 and PC games in the same time periods. The reason might be conveyed into some big releases (such as GTA V or Assassin's Creed) which in the time period we are interested in (2010s) had tendency to have parallel reseales on several platforms. This fact could cause such a correlation.
# 
# Another reason enforcing this behaviour might be that the target market of our show are kids playing games. Or in even broader context: people tend to play games rather than buying DVDs or CDs with music (these in Russia can be downloaded for free from Torrents), so there is no motivation in Russia in buying these things. That's why maybe this tendency contribute to the rise in sales in the beginning of autumn and also in the beginning of summer.

# ### By a month of the year

# In[617]:


agg_df = total_price_to_date_pd_df[
    ['item_category_name', 'month_of_the_year', 'total_daily_sale']
].groupby(['item_category_name', 'month_of_the_year']).sum()


# In[618]:


plt.figure(figsize=(20,10))
ax = sns.lineplot(
    data=agg_df,
    x='month_of_the_year',
    y='total_daily_sale',
    hue='item_category_name',
    palette='twilight',
    marker='o'
)
ax.yaxis.set_major_formatter(formatter)
plt.title('Sales by biggest 5 categories per week of a year')
plt.xlabel('Month of the year')
plt.ylabel('Sales in rubles')
plt.grid()
plt.savefig(os.path.join(output_data_plots_path, 'top_5_categories_sales_per_each_week_of_the_year.png'))
plt.show()


# In contrast to other graphs we have seen so far, this one shows that the sales of DVDs, CDs and Blu-Ray are pretty stable. We can then predict their future behaviour more confidently and make some better-grounded assumptions about this part of our goods. These categories can be also considered as stable in demand, so their XYZ classification might be X. 
# 
# We cannot claim that the need for PS3 games is stable. We see that PS3 games contribute more to our income, but the demand is more variable and we cannot expect the same number of sold items each month. So we are more prone to other factors that have higher influence on these goods (like new consoles, new games, holidays, popularity of these games and brands).

# ## Summary

# We have conducted a profound study and analysed our data from lots of perspectives and angles. We have seen that by taking a look at the same data with respect to different time span, time range, categories, we obtain different insights which might help in defining the strategy of our business, our targets and goals.
# 
# There are lots of messages for marketing department that I've addressed throughout the analysis itself, so here I will be brief and hopefully clear in my thoughts.
# 
# First of all, we have several categories which are more predictable in terms of future demand and sales. They do not vary so much during the year and so the only thing we have to do is to keep an eye on considerable supply of these products throughout the next planned strategy time span. We have seen that there are such products (like Pink Floyd CDs, Deep Purple CDs, GTA SA game), which despite the fact that they are pretty old, they are still stable in terms of selling capabilities, that's why don't wish to run short of them.
# 
# We have also observed that we mostly benefit from selling games and from some event happening in game industry. So the message for the marketing team is to be in the picture of all the stuff happening in these spheres. The team has to be acquainted with the target market, be familiar of hot reseases and the upcoming events. Only by staying informed can we keep the sales high and hold attention of our customers. We have also to keep in mind that not only the games for PCs and consoles are being sold pretty well, but the consoles themselves, particularly on the eve of major holidays and events in social lives of our target market. That's why the team has to prepare some campaigns supporting this demand and at the same time take care of not running short of products we aim to sell to our customers.
# 
# There are also some peculiarities to take into consideration like targeting some ads towards our customers more agressively one week before the month ends or even some time before since it's the most profitable time of each month. 
# 
# As a result, the marketing team has lots of food for thought and will have by far less painful process of coming up with the ideas for future ads and marketing campaigns for supporting and rising our sales. 

# In[ ]:




