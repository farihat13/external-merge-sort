#!/usr/bin/python3


import pandas as pd
import matplotlib.pyplot as plt
import sys


# Command line argument
csv_file = sys.argv[1]

# Load the data from the output file
data = pd.read_csv(csv_file, header=0)
#strip the leading and trailing spaces from the column names
data.columns = data.columns.str.strip()
#strip the leading and trailing spaces from the column values
data = data.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

print(data.head())
print(data.columns)

# Convert the timestamp to datetime format for better plotting
data['Timestamp'] = pd.to_datetime(data['Timestamp'])
data['RSS (MB)'] = data['RSS (KB)'].astype(float)/1024
data['VSZ (MB)'] = data['VSZ (KB)'].astype(float)/1024

# Plotting
fig, ax1 = plt.subplots()

# Create a plot for memory usage (RSS and VSZ)
color = 'tab:red'
ax1.set_xlabel('Timestamp')
ax1.set_ylabel('Memory (MB)', color=color)
ax1.plot(data['Timestamp'], data['RSS (MB)'], label='RSS (MB)', color='red')
ax1.plot(data['Timestamp'], data['VSZ (MB)'], label='VSZ (MB)', color='blue')
ax1.tick_params(axis='y', labelcolor=color)
ax1.legend(loc='upper left')

# Create a second y-axis for CPU usage
ax2 = ax1.twinx()
color = 'tab:blue'
ax2.set_ylabel('CPU (%)', color=color)
ax2.plot(data['Timestamp'], data['CPU (%)'], label='CPU (%)', color='green')
ax2.tick_params(axis='y', labelcolor=color)
ax2.legend(loc='upper right')

# Title and show
plt.title('Memory and CPU Usage Over Time')
plt.savefig(csv_file+'.png')
