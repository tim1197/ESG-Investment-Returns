import pandas as pd
import numpy as np
import os


class SP500ESGIndexReplication:
    def __init__(self):
        pass

    def replicate_index(self, path: str, excluded_companies: list) -> pd.DataFrame:
        """
        Combine steps to replicate S&P 500 ESG back to base date April 30th, 2010
        :param path: String to raw data
        :return: Cumulative index development until today
        """

        data_sp500 = self.data_preparation(path)
        data_sp500 = self.evaluate_industry_exposure(data_sp500)
        sp500_rebalance_factors = self.get_rebalance_factors_on_reference_date(
            data_sp500
        )
        index_compositions = self.get_index_composition(
            sp500_rebalance_factors, data_sp500, excluded_companies
        )
        cumulative_index_returns = self.index_replication(
            index_compositions, data_sp500
        )

        return cumulative_index_returns

    def data_preparation(self, path: str) -> pd.DataFrame:
        """
        Prepare the data to replicate S&P 500 ESG back to the Base Date on April 30th, 2010.
        :param path: String to raw data
        :return: DF of S&P 500 raw data
        """

        data_sp500 = (
            pd.read_csv(path, dtype=str)
            .drop(columns="Unnamed: 0")
            .set_index(["permno", "date"])
            .rename(
                columns={
                    "environmental_normalized": "environmental",
                    "social_normalized": "social",
                    "governance_normalized": "governance",
                    "esg_normalized": "esg",
                }
            )
            .sort_index()
        )
        # Exclude data before January 1st, 2010 as base date of S&P500 ESG is April 30th, 2010
        data_sp500 = data_sp500.loc[
            data_sp500.index.get_level_values("date") >= "2010-01-01"
        ]

        # Setting the datetime format
        data_sp500.index = pd.MultiIndex.from_arrays(
            [
                data_sp500.index.get_level_values("permno"),
                pd.to_datetime(data_sp500.index.get_level_values("date")),
            ],
            names=["permno", "date"],
        )

        return data_sp500

    def evaluate_industry_exposure(self, data_sp500: pd.DataFrame) -> pd.DataFrame:
        """
        Evaluate if companies have exposure to industries that are excluded through the index methodology.
        :param data_sp500: DF of S&P 500 raw data
        :return: S&P 500 data with a new column for industry exposure
        """

        # List that defines which industries are not ESG conform
        non_esg = ["CW", "FA", "MC", "OS", "TC", "TP"]

        # We assume that companies with nan values are esg conform
        data_sp500.loc[:, "industry exposure"] = data_sp500[
            "industry exposure"
        ].replace(np.nan, "0")

        # Function to check if any string in non_esg is present in a given row of industry exposure
        def check_non_esg(row):
            return any(substring in row["industry exposure"] for substring in non_esg)

        data_sp500["non esg"] = data_sp500.apply(check_non_esg, axis=1).astype(int)

        return data_sp500

    def get_rebalance_factors_on_reference_date(self, data_sp500: pd.DataFrame) -> dict:
        """
        Get the rebalance factors of each company on the reference day (last trading day in march) for rebalancing.
        :param data_sp500: Prepared S&P 500 data
        :return: Dict with dfs for each rebalance factor and company on reference day
        """

        # Filter for last trading day in March for each year and permno
        march_data = data_sp500.loc[
            data_sp500.index.get_level_values("date").month == 3
        ]
        last_trading_day_march = march_data.groupby(
            [
                march_data.index.get_level_values("permno"),
                march_data.index.get_level_values("date").year,
            ]
        ).tail(1)

        # Creating a pivot df containing market cap and ESG scores
        pivot_df = (
            last_trading_day_march.reset_index()
            .pivot(
                index="date",
                columns="permno",
                values=["mktcap", "environmental", "social", "governance", "esg"],
            )
            .astype(float)
        )

        # Separate each factor into its own DF and store in a dictionary
        rebalance_factors = {}
        for factor in ["mktcap", "environmental", "social", "governance", "esg"]:
            rebalance_factors[factor] = pivot_df[factor]

        # Exclude wrong rebalancing dates due to merger or bankruptcy of companies
        for key in rebalance_factors:
            rebalance_factors[key] = rebalance_factors[key].dropna(thresh=300, axis=0)

        return rebalance_factors

    def get_index_composition(
        self,
        sp500_rebalance_factors: dict,
        data_sp500: pd.DataFrame,
        excluded_companies: list,
    ) -> dict:
        """
        Create the index compositions based on rebalance factors for the S&P 500 ESG index based on the reference date for rebalancing
        :param sp500_rebalance_factors: DF with rebalance factors for each company on reference day
        :param sp500_data: SP500 data including industry exposure
        :param excluded_companies: list with excluded companies
        :return: Index composition for the S&P 500 ESG index based on the reference date for rebalancing
        """

        # Get list of permnos that won't be part of the S&P 500 ESG index
        non_esg_permnos = set(
            data_sp500[data_sp500["non esg"] == 1]
            .index.get_level_values("permno")
            .tolist()
        )

        index_composition_dict = {}
        # Get the market cap DataFrame
        mktcap_df = sp500_rebalance_factors.get("mktcap")
        if mktcap_df is None:
            raise ValueError(
                "Market capitalization data (mktcap) is required in rebalance_factors."
            )

        # Determine the constituents based on market capitalization
        mktcap_composition = pd.DataFrame()
        top_500_permnos_by_date = {}
        for date in mktcap_df.index:
            constituents = mktcap_df.loc[
                date
            ].copy()  # Ensure to make a copy to avoid modifying original data
            # Select the top 500 by market capitalization
            constituents = (
                constituents.sort_values(ascending=False)
                .head(500)
                .to_frame(name="mktcap")
                .reset_index()
            )
            # Exclusion of companies without or missing 10K's
            constituents = constituents[
                ~constituents["permno"].isin(excluded_companies.astype(str))
            ]
            constituents.set_index("permno", inplace=True)
            # Exclude companies based on non_esg_permnos
            constituents.drop(
                index=[
                    permno for permno in non_esg_permnos if permno in constituents.index
                ],
                inplace=True,
            )
            # Store the top 500 permnos for this date
            top_500_permnos_by_date[date] = constituents.index.tolist()
            # Get constituents weights including capping
            constituent_weight = (
                (constituents["mktcap"] / constituents["mktcap"].sum())
                .to_frame(name=date)
                .T
            )
            constituent_weight.index = [date]
            mktcap_composition = pd.concat(
                [mktcap_composition, constituent_weight], axis=0
            )

        # Store the market capitalization composition
        index_composition_dict["mktcap"] = mktcap_composition

        # Iterate over each rebalance factor
        for factor, factor_df in sp500_rebalance_factors.items():
            if factor == "mktcap":
                continue  # Skip mktcap as it's already processed
            factor_composition = pd.DataFrame()
            for date in factor_df.index:
                # Use the top 500 permnos determined from the market cap
                top_500_permnos = top_500_permnos_by_date.get(date, [])
                constituents = (
                    factor_df.loc[date, top_500_permnos].to_frame(name=factor).T
                )
                constituents.index = [date]
                # Calculate the weights based on the factor
                factor_weight_sum = constituents.sum(axis=1)
                factor_weight = constituents.div(factor_weight_sum, axis=0)
                factor_weight.index = [date]
                factor_composition = pd.concat(
                    [factor_composition, factor_weight], axis=0
                )

            # Store the factor composition
            index_composition_dict[factor] = factor_composition
        return index_composition_dict

    def index_replication(
        self, index_compositions: dict, data_sp500: pd.DataFrame
    ) -> dict:
        """
        Replicate S&P 500 ESG back to base date April 30th, 2010
        :param index_compositions: Dict with index composition for each rebalance factor for the S&P 500 ESG index based on the reference date for rebalancing
        :param data_sp500: DF of S&P 500 raw data
        :return: Cumulative index development until today for each rebalance factor
        """

        cumulative_index_returns = {}

        for factor, index_composition in index_compositions.items():
            # Create a df that only contains the companies returns
            returns_df = data_sp500.reset_index().pivot(
                index="date", columns="permno", values="ret"
            )

            index_returns = pd.DataFrame()
            for year in index_composition.index.year:
                # Calculate start and end dates for the rebalancing
                start_date = pd.Timestamp(year, 5, 1)
                end_date = start_date.replace(year=start_date.year + 1, month=4, day=30)

                # Get index weights for the current year
                index_weights_year = index_composition.loc[
                    index_composition.index.year == year
                ].dropna(axis=1)

                # Get daily weighted returns per company
                weighted_returns = pd.DataFrame(
                    index=returns_df.loc[start_date:end_date].index,
                    columns=returns_df.columns,
                )

                for permno in index_weights_year.columns:
                    weighted_returns[permno] = returns_df.loc[start_date:end_date][
                        permno
                    ].astype(float) * pd.Series(
                        index_weights_year[permno].values[0],
                        index=returns_df.loc[start_date:end_date][permno].index,
                    )

                daily_index_return = weighted_returns.sum(axis=1)
                index_returns = pd.concat([index_returns, daily_index_return])

            cumulative_index_return = (
                (1 + index_returns)
                .cumprod()
                .rename(columns={0: "Cumulative Index Return"})
            )
            # Normalize returns and create base value of 100
            cumulative_index_return = (
                cumulative_index_return / cumulative_index_return.iloc[0]
            ) * 100
            # Add the cumulative index return to the dictionary
            factor_name = factor.replace("_index_composition", "")
            cumulative_index_returns[factor_name] = cumulative_index_return

        return cumulative_index_returns
