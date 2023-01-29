import numpy as np
import scipy.stats as stats

# Define the two groups of data
group_a = [1, 2, 3, 4, 5]
group_b = [2, 3, 4, 5, 6]

# Perform the t-test
t_test_results = stats.ttest_ind(group_a, group_b)

# Print the p-value
print("p-value: ", t_test_results.pvalue)

# Check if the p-value is less than 0.05 (the significance level)
if t_test_results.pvalue < 0.05:
    print("Reject the null hypothesis.")
else:
    print("Fail to reject the null hypothesis.")
