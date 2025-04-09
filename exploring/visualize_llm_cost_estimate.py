import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

df = pd.read_csv("llm_cost_estimate.csv")

# Set Seaborn style
sns.set_theme(style="whitegrid")

# Plot 1: Number of Posts and Top-Level Comments per Year
plt.figure(figsize=(10, 5))
sns.barplot(data=df, x="year", y="number_of_posts", color="blue", label="Number of Posts")
sns.barplot(data=df, x="year", y="number_of_top_level_comments", color="red", alpha=0.7, label="Number of Top-Level Comments")
plt.xlabel("Year")
plt.ylabel("Count")
plt.title("Number of Posts and Top-Level Comments per Year")
plt.legend()
plt.show()

# Plot 2: Word Count and Token Count for Posts per Year
plt.figure(figsize=(10, 5))
df_melted_posts = df.melt(id_vars=["year"], value_vars=["words_posts", "token_posts"], var_name="Metric", value_name="Count")
sns.barplot(data=df_melted_posts, x="year", y="Count", hue="Metric")
plt.xlabel("Year")
plt.ylabel("Count")
plt.title("Word and Token Count for Posts per Year")
plt.legend(title="Metric")
plt.show()

# Plot 3: Word Count and Token Count for Top-Level Comments per Year
plt.figure(figsize=(10, 5))
df_melted_comments = df.melt(id_vars=["year"], value_vars=["words_tl_comments", "token_tl_comments"], var_name="Metric", value_name="Count")
sns.barplot(data=df_melted_comments, x="year", y="Count", hue="Metric")
plt.xlabel("Year")
plt.ylabel("Count")
plt.title("Word and Token Count for Top-Level Comments per Year")
plt.legend(title="Metric")
plt.show()