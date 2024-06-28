import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


def plot_cumulative_returns(cum_return_data, title, save_path):
    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(12, 6))
    
    for factor, cum_return_df in cum_return_data.items():
        sns.lineplot(
            x=cum_return_df.index,
            y="Cumulative Index Return",
            data=cum_return_df,
            label=factor,
        )
    
    years = mdates.YearLocator(2)  # Set the x-axis ticker to show every second year
    plt.gca().xaxis.set_major_locator(years)
    
    formatter = mdates.DateFormatter("%Y")  # Set the x-axis formatter to display only the year
    plt.gca().xaxis.set_major_formatter(formatter)
    
    plt.title(title, fontsize=16)
    plt.xlabel("Year", fontsize=16)
    plt.ylabel("Index Development", fontsize=16)
    plt.legend(title="Rebalancing Factor")
    plt.tight_layout()
    plt.savefig(save_path)
    plt.show()