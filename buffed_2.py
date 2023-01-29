import numpy as np
import pandas as pd
import scipy.stats as stats
import random

# Define the conversion rate for each group
conversion_rate_a = 0.1
conversion_rate_b = 0.12

# Define the sample size for each group
sample_size_a = 1000
sample_size_b = 1000

# Generate the data for each group
group_a = np.random.binomial(1, conversion_rate_a, sample_size_a)
group_b = np.random.binomial(1, conversion_rate_b, sample_size_b)

# Calculate the conversion rate for each group
conversion_rate_a = group_a.mean()
conversion_rate_b = group_b.mean()

# Perform a t-test on the conversion rates
t_test_results = stats.ttest_ind(group_a, group_b)

# Print the p-value
print("p-value: ", t_test_results.pvalue)

# Check if the p-value is less than 0.05 (the significance level)
if t_test_results.pvalue < 0.05:
    print("Reject the null hypothesis.")
    print("Group B has a higher conversion rate than Group A.")
else:
    print("Fail to reject the null hypothesis.")
    print("There is not enough evidence to conclude that Group B has a higher conversion rate than Group A.")
