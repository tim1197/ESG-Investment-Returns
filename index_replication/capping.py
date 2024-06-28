import pandas as pd
import numpy as np
from loguru import logger


class OneDimensionCapping:
    """
    This class is performing a one dimensional capping, e.g. issuer.

    :param data: The dataframe as input it should include mktcap column on which the capping will run
    :param capping_percent: this is the maximum capping percentage
    :param dimension: The dimension to be capped e.g. issuer --> This should be exactly the name of column in
                      the dataframe even the capital vs. small letters should be respected
    """

    def __init__(self, data: pd.DataFrame, capping_percent: float, dimension: str):

        self.dataframe = data
        self.capping_percent = capping_percent
        self.dimension = dimension
        self.iterations = 0
        self.error = 0.001
        logger.info(
            f"Initiating the capping to cap {self.dimension} with a cap percent of {self.capping_percent}"
        )

    def checking_capping_plausibility(self):
        """
        This function checks if the capping is plausible or not. e.g. If u have 5 companies and you want to cap each
        company to 10%; this is not plausible as the weights will sum up to less than 50%
        """
        group_object = self.dataframe.groupby(self.dimension).sum(numeric_only=True)
        num_dimension_constituents = len(group_object.index)
        minimum_plausible_cap_percent = 1 / num_dimension_constituents

        if self.capping_percent < minimum_plausible_cap_percent:
            raise Exception(
                "The capping percentage is lower than the minimum needed to run a capping algorithm"
            )

    def prep_capping(self):
        """
        Adding the necessary columns to the dataframe that would be necessary to run the capping
        """
        self.dataframe["initial_amount"] = self.dataframe["mktcap"]
        self.dataframe["capped_amount"] = self.dataframe["mktcap"]
        self.dataframe[["capping_flag", "cap_factor"]] = 1

        return self.dataframe

    def checking_capping_result(self):
        """
        This calculation is essential to make sure that the capping was executed successfully. The sum of MV before
        capping should be exactly the salem like after capping.
        """
        sum_capped_amount = round(self.dataframe["capped_amount"].sum(), 0)
        sum_initial_amount = round(self.dataframe["initial_amount"].sum(), 0)

        if sum_capped_amount - sum_initial_amount <= 5:
            logger.success(
                f"The Capping has been executed successfully with {self.iterations} iteration(s)!"
            )
        else:
            logger.error(
                "The Capping ended in a wrong result! Please check the data and model parameters again!"
            )

    def do_capping(self):
        """
        This function encapsulates the capping algorithm.
        """

        # Calling the functions to assure plausibility of the capping and to add the necessary columns to dataframe
        self.checking_capping_plausibility()
        self.dataframe = self.prep_capping()

        # Calculating the maximum Market Capitalization accepted for any dimension
        capped_market_value_amount = self.capping_percent * (
            self.dataframe["initial_amount"].sum()
        )
        # Getting the sums of all members of the dimension included in the index
        group_object = self.dataframe.groupby(self.dimension).sum(numeric_only=True)

        # Now starts the while loop tha performs the capping by checking if any dimension MV exceeds the cap
        while any(
            group_object.loc[:, "capped_amount"]
            > capped_market_value_amount + self.error
        ):
            self.iterations = self.iterations + 1

            for i in group_object.index:
                # For any dimension exceeding the cap; the capping procedure is applied
                # First we get the amount exceeding the cap and then we set the capping flag of the dimension to 0
                # so it is not capped again if the loop is to continue
                if group_object.loc[i, "capped_amount"] > (
                    capped_market_value_amount + self.error
                ):
                    over_amount = (
                        group_object.loc[i, "capped_amount"]
                        - capped_market_value_amount
                    )
                    self.dataframe["capping_flag"] = np.where(
                        self.dataframe[self.dimension] == i,
                        0,
                        self.dataframe["capping_flag"],
                    )

                    # Calculating the down factor which will decrease the weightings of the overly weighted members
                    down_factor = (
                        capped_market_value_amount
                        / group_object.loc[i, "capped_amount"]
                    )
                    self.dataframe["capped_amount"] = np.where(
                        self.dataframe[self.dimension] == i,
                        self.dataframe["capped_amount"] * down_factor,
                        self.dataframe["capped_amount"],
                    )

                    # Calculating the up factor which will be used to increase the weightings of the remaining companies
                    sum_amount_remaining_members = self.dataframe.loc[
                        self.dataframe["capping_flag"] == 1
                    ]["capped_amount"].sum()

                    up_factor = (
                        sum_amount_remaining_members + over_amount
                    ) / sum_amount_remaining_members
                    self.dataframe["capped_amount"] = np.where(
                        self.dataframe["capping_flag"] == 1,
                        self.dataframe["capped_amount"] * up_factor,
                        self.dataframe["capped_amount"],
                    )

                    # Recalculating the dimension sums after the application of the capping iteration
                    group_object = self.dataframe.groupby(self.dimension).sum(
                        numeric_only=True
                    )

            self.dataframe["cap_factor"] = (
                self.dataframe["capped_amount"] / self.dataframe["initial_amount"]
            )
        self.checking_capping_result()

        return self.dataframe

    def run_capping(self):
        return self.do_capping()
