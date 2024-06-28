from sp500esg_replication import SP500ESGIndexReplication
from dax50esg_repl import DAX50ESGIndexReplication
from plot_cum_returns import plot_cumulative_returns
import pandas as pd


def main():
    sp500esg_index_replication = SP500ESGIndexReplication()
    dax50esg_index_replication = DAX50ESGIndexReplication()

    excluded_companies_sp500_esg = pd.read_excel(
        r"YOUR_PATH"
    )
    cum_return_sp500_esg = sp500esg_index_replication.replicate_index(
        path=r"YOUR_PATH",
        excluded_companies=excluded_companies_sp500_esg["permno"],
    )

    # Save the cumulative returns of S&P 500 ESG
    for name, df in cum_return_sp500_esg.items():
        df.to_csv(
            f"YOUR_PATH",
            index=True,
        )

    excluded_companies_dax_50_esg = ["DE000GSW1111", "DE000CLS1001", "DE0007472060"]
    cum_return_dax50_esg = dax50esg_index_replication.replicate_index(
        path=r"YOUR_PATH",
        excluded_companies=excluded_companies_dax_50_esg,
    )

    # Save the cumulative returns of DAX 50 ESG
    for name, df in cum_return_dax50_esg.items():
        df.to_csv(
            f"YOUR_PATH",
            index=True,
        )

    plot_cumulative_returns(
        cum_return_sp500_esg,
        title="Development of the S&P 500 ESG Indices",
        save_path=r"YOUR_PATH",
    )

    plot_cumulative_returns(
        cum_return_dax50_esg,
        title="Development of the DAX 50 ESG Indices",
        save_path=r"YOUR_PATH",
    )


if __name__ == "__main__":
    main()
