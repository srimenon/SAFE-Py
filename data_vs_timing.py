import matplotlib.pyplot as plt

data = [186, 318, 606, 1609, 2168, 2538, 3191, 3775, 3843, 4020, 4132, 4171, 4198, 4223, 4328, 4835]
server = [1.05, 1.18, 2.52, 11.45, 16.42, 22.85, 32.79, 48.2, 50.23, 54.69, 57.67, 58.87, 57.28, 0, 0, 0]
download = [0.14, 0.1, 0.3, 0.77, 1.42, 0.6, 2.12, 0.7, 1.28, 1.65, 0.6, 1.23, 0.65, 0, 0, 0]

plt.plot(data, server, label="Server timing")
# plt.plot(data, download, label="Download timing")
plt.xlabel("Number of datapoints")
plt.ylabel("Time (seconds)")
plt.legend()
plt.show()