import pandas as pd
import numpy as np
from datetime import timedelta
from capping import OneDimensionCapping


class DAX50ESGIndexReplication:
    def __init__(self):
        pass

    def replicate_index(self, path: str, excluded_companies: list) -> pd.DataFrame:
        """
        Combine steps to replicate DAX 50 ESG back to base date September 24th, 2012
        :param path: String to raw data
        :return: Cumulative index development until today
        """

        data_dax50 = self.data_preparation(path, excluded_companies)
        data_dax50 = self.evaluate_industry_exposure(data_dax50)
        dax50_rebalance_factors = self.get_mktcap_on_reference_date(data_dax50)
        index_composition_dict = self.get_index_composition(
            dax50_rebalance_factors, data_dax50
        )
        cumulative_index_return = self.index_replication(
            index_composition_dict, data_dax50
        )

        return cumulative_index_return

    def data_preparation(self, path: str, excluded_companies: list) -> pd.DataFrame:
        """
        Prepare the data to replicate DAX 50 ESG back to base date September 24th, 2012
        :param path: String to raw data
        :return: DF of DAX 50 ESG raw data
        """

        # Load data and set index, isin is an unique company identifier
        data_dax50 = pd.read_csv(path).drop(columns="Unnamed: 0")
        # Drop excluded companies
        data_dax50 = data_dax50[~data_dax50["isin"].isin(excluded_companies)]
        # Get returns for the isins
        data_dax50["return"] = data_dax50.groupby('isin')['price'].apply(lambda x: (x / x.shift(1)) - 1).reset_index(level=0, drop=True)
        data_dax50 = data_dax50.set_index(["isin", "date"]).rename(
            columns={
                "market capitalization in milion": "mktcap",
                "environmental_normalized": "environmental",
                "social_normalized": "social",
                "governance_normalized": "governance",
                "esg_normalized": "esg",
            }
        )
        # Exclude data before September 21th, 2012 as this is the reference date for the base date of Dax 50 ESG
        data_dax50 = data_dax50.loc[
            data_dax50.index.get_level_values("date") >= "2012-09-21"
        ]
        # Setting the datetime format
        data_dax50.index = pd.MultiIndex.from_arrays(
            [
                data_dax50.index.get_level_values("isin"),
                pd.to_datetime(data_dax50.index.get_level_values("date")),
            ],
            names=["isin", "date"],
        )

        return data_dax50

    def evaluate_industry_exposure(self, data_dax50: pd.DataFrame) -> pd.DataFrame:
        """
        Evaluate if companies have exposure to industries that are excluded through the index methodology.
        :param data_dax50: DF of raw company data
        :return: Company data with a new column for industry exposure
        """

        # List that defines which industries are not ESG conform
        non_esg = ["CW", "FA", "MC", "NP", "OS", "TC", "TP"]

        # We assume that companies with nan values are esg conform
        data_dax50.loc[:, "industry exposure"] = (
            data_dax50["industry exposure"]
            .replace(np.nan, "0")
            .replace("#N/A Invalid Security", "0")
        )

        # Function to check if any string in non_esg is present in a given row of industry exposure
        def check_non_esg(row):
            return any(substring in row["industry exposure"] for substring in non_esg)

        data_dax50["non esg"] = data_dax50.apply(check_non_esg, axis=1).astype(int)

        return data_dax50

    def get_mktcap_on_reference_date(self, data_dax50: pd.DataFrame) -> dict:
        """
        Get the market capitalization and ESG coomunication scores of each company on the reference days (last Friday) before rebalancing.
        :param data_dax50: Prepared DAX 50 data
        :return: Dict with DataFrames for each rebalance factor
        """

        months = [3, 6, 9, 12]  # March, June, September, December
        years = [2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]
        dates = []

        for month in months:
            for year in years:
                # Get the start and end of the month
                start_of_month = pd.Timestamp(year=year, month=month, day=1)
                end_of_month = start_of_month + pd.offsets.MonthEnd(0)

                # Generate a date range for the month
                dates_in_month = pd.date_range(
                    start=start_of_month, end=end_of_month, freq="B"
                )

                # Reverse the date range and find the second last Friday of the month
                friday_count = 0
                for date in reversed(dates_in_month):
                    if date.dayofweek == 4:  # Check if it's a Friday
                        friday_count += 1
                        if friday_count == 2:  # If it's the second Friday
                            if (
                                date.strftime("%Y-%m-%d") == "2021-12-24"
                            ):  # Check manually for Christmas 2021
                                date += pd.Timedelta(days=3)
                            dates.append(date)
                            break

        data_dax50 = data_dax50.reset_index()

        # Create a dictionary to store the DataFrames
        rebalance_factors = {}

        # Create pivot tables for each rebalance factor
        factors = [
            "mktcap",
            "environmental",
            "social",
            "governance",
            "esg",
        ]
        for factor in factors:
            pivot_df = data_dax50[data_dax50["date"].isin(dates)].pivot(
                index="date", columns="isin", values=factor
            )
            rebalance_factors[factor] = pivot_df

        return rebalance_factors

    def get_index_composition(
        self, rebalance_factors: dict, data_dax50: pd.DataFrame
    ) -> dict:
        """
        Create the index composition for the DAX 50 ESG index based on the reference date for rebalancing
        :param rebalance_factors: Dict with DataFrames for each rebalance factor
        :param data_dax50: Prepared DAX 50 data
        :return: Dict with index composition for the DAX 50 ESG index based on the reference date for rebalancing for each factor
        """

        # Get list of ISINs that won't be part of the DAX 50 ESG index
        non_esg_isins = set(
            data_dax50[data_dax50["non esg"] == 1]
            .index.get_level_values("isin")
            .tolist()
        )

        # Initialize a dictionary to store the index composition for each factor
        index_composition_dict = {}

        # Get the market cap DataFrame
        mktcap_df = rebalance_factors.get("mktcap")
        if mktcap_df is None:
            raise ValueError(
                "Market capitalization data (mktcap) is required in rebalance_factors."
            )

        # Determine the constituents based on market capitalization
        mktcap_composition = pd.DataFrame()
        top_50_isins_by_date = {}
        for date in mktcap_df.index:
            constituents = mktcap_df.loc[
                date
            ].copy()  # Ensure to make a copy to avoid modifying original data
            # Exclude companies based on non_esg_isins
            constituents.drop(
                index=[isin for isin in non_esg_isins if isin in constituents.index],
                inplace=True,
            )
            # Select the top 50 by market capitalization
            constituents = (
                constituents.sort_values(ascending=False)
                .head(50)
                .to_frame(name="mktcap")
                .reset_index()
            )
            # Store the top 50 ISINs for this date
            top_50_isins_by_date[date] = constituents["isin"].tolist()
            # Apply capping to market capitalization
            constituents_overview = OneDimensionCapping(
                data=constituents, capping_percent=0.07, dimension="isin"
            ).run_capping()
            constituents_overview.set_index("isin", inplace=True)
            # Get constituents weights including capping
            constituent_weight = (
                constituents_overview["capped_amount"]
                / constituents_overview["mktcap"].sum()
            )
            mktcap_composition = pd.concat(
                [mktcap_composition, constituent_weight.to_frame(name=date).T], axis=0
            )

        # Store the market capitalization composition
        index_composition_dict["mktcap"] = mktcap_composition

        # Use the same constituents for other factors and calculate their weights
        for factor, factor_df in rebalance_factors.items():
            if factor == "mktcap":
                continue  # Skip mktcap as it's already processed
            factor_composition = pd.DataFrame()
            for date in factor_df.index:
                # Use the top 50 ISINs determined from the market cap
                top_50_isins = top_50_isins_by_date.get(date, [])
                constituents = (
                    factor_df.loc[date, top_50_isins].dropna().to_frame(name=factor).T
                )
                constituents.index = [date]
                # Calculate the weights based on the factor
                factor_weight_sum = constituents.sum(axis=1)
                factor_weight = constituents.div(factor_weight_sum, axis=0)
                factor_composition = pd.concat(
                    [factor_composition, factor_weight], axis=0
                )

            # Store the factor composition
            index_composition_dict[factor] = factor_composition

        return index_composition_dict

    def index_replication(
        self, index_composition_dict: dict, data_dax50: pd.DataFrame
    ) -> dict:
        """
        Replicate DAX 50 ESG back to base date September 24th, 2012 for each rebalance factor
        :param index_composition_dict: Dict with index compositions for each rebalance factor
        :param data_dax50: DF of DAX 50 raw data
        :return: Dict with cumulative index returns for each rebalance factor
        """

        # Create a df that only contains the companies returns
        returns_df = data_dax50.reset_index().pivot(
            index="date", columns="isin", values="return"
        )

        cumulative_returns_dict = {}

        for factor, index_composition in index_composition_dict.items():
            index_returns = pd.DataFrame()
            # Get the next business day for each timestamp in index_composition
            next_business_days = index_composition.index.map(
                lambda x: x + pd.offsets.BDay()
            )

            for i, date in enumerate(index_composition.index):
                # Ensure that we're not at the last index to avoid an out-of-range error
                if i < len(index_composition.index) - 1:
                    start_date = next_business_days[
                        i
                    ]  # Use the next business day as the start date
                    end_date = index_composition.index[
                        i + 1
                    ]  # Use the next timestamp as the end date
                else:
                    start_date = next_business_days[
                        i
                    ]  # Use the next business day as the start date
                    end_date = pd.to_datetime(
                        "2023-12-31"
                    )  # Index calculation ends at 31st of December 2023

                # Get index weights for the current period
                index_weights_period = index_composition.loc[date].dropna()

                # Get daily weighted returns per company
                weighted_returns = pd.DataFrame(
                    index=returns_df.loc[start_date:end_date].index,
                    columns=index_weights_period.index,
                )
                for isin in index_weights_period.index:
                    weighted_returns[isin] = (
                        returns_df.loc[start_date:end_date][isin]
                        * index_weights_period.loc[isin]
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

            cumulative_returns_dict[factor] = cumulative_index_return

        return cumulative_returns_dict
