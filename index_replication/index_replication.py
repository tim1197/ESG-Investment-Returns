from sp500esg_replication import SP500ESGIndexReplication
from dax50esg_repl import DAX50ESGIndexReplication
from plot_cum_returns import plot_cumulative_returns
import pandas as pd


def main():
    sp500esg_index_replication = SP500ESGIndexReplication()
    dax50esg_index_replication = DAX50ESGIndexReplication()

    
    excluded_companies_sp500_esg = pd.read_excel(r"/Users/timmuller/Documents/Uni/Master/Thesis/SP500ESG/exclduded_companies_SP500ESG.xlsx")
    cum_return_sp500_esg = sp500esg_index_replication.replicate_index(
        path=r"/Users/timmuller/Documents/Uni/Master/Thesis/SP500ESG/sp500_comp_incl_esg_scores.csv",
        excluded_companies=excluded_companies_sp500_esg["permno"]
    )
    
    # Save the cumulative returns of S&P 500 ESG
    for name, df in cum_return_sp500_esg.items():
        df.to_csv(f"/Users/timmuller/Documents/Uni/Master/Thesis/SP500ESG/results/S&P500ESG_cum_return_{name}.csv", index=True)
    
    excluded_companies_dax_50_esg = ["DE000GSW1111", "DE000CLS1001", "DE0007472060"]
    cum_return_dax50_esg = dax50esg_index_replication.replicate_index(
        path=r"/Users/timmuller/Documents/Uni/Master/Thesis/DAX50ESG/data_dax_esg_scores_normalized.csv",
        excluded_companies=excluded_companies_dax_50_esg
    )
    
    # Save the cumulative returns of DAX 50 ESG
    for name, df in cum_return_dax50_esg.items():
        df.to_csv(f"/Users/timmuller/Documents/Uni/Master/Thesis/DAX50ESG/results/DAX50ESG_cum_return_{name}.csv", index=True)
    
    plot_cumulative_returns(
        cum_return_sp500_esg,
        title="Development of the S&P 500 ESG Indices",
        save_path=r"/Users/timmuller/Documents/Uni/Master/Thesis/SP500ESG/plots/SP500ESG_cum_return.png"
    )

    plot_cumulative_returns(
        cum_return_dax50_esg,
        title="Development of the DAX 50 ESG Indices",
        save_path=r"/Users/timmuller/Documents/Uni/Master/Thesis/DAX50ESG/plots/DAX50ESG_cum_return.png"
    )


if __name__ == "__main__":
    main()
