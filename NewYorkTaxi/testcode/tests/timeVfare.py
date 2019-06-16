import pandas as pd
import matplotlib.pyplot as plt


df = pd.DataFrame()
df1 = pd.read_csv("graphpoints.csv", header=None)
fig, ax = plt.subplots()
plt.bar(df1[0], df1[3], align='center', alpha=0.5, label='No. of trips')
plt.set_ylabel = 'No. of trips'
axes2 = plt.twinx()
axes2.plot(df1[0],df1[2], color='red', label='Average Speed')
axes2.legend(loc=1)
axes2.tick_params(axis='y', colors='red')
ax.set_xlabel('Time')
ax.tick_params(axis='y', colors='blue', which='major', pad=15)

every_nth = 4
for n, label in enumerate(ax.xaxis.get_ticklabels()):
    if n % every_nth != 0:
        label.set_visible(False)


axes3 = plt.twinx()
axes3.plot(df1[0],df1[1], color='green', label='Average Fare')
axes3.legend(loc=2)
axes3.tick_params(axis='y', colors='green')
plt.savefig('finalgraph.png')
plt.show()






#plt.savefig("timeVcountVspeed.png")

# fig, ax = plt.subplots()
# plt.bar(df1[0], df1[1], align='center', alpha=0.5)
# plt.ylabel = 'No. of trips'
# axes2 = plt.twinx()
# axes2.plot(df1[0],df1[2], color='red')
# every_nth = 4
# for n, label in enumerate(ax.xaxis.get_ticklabels()):
#     if n % every_nth != 0:
#         label.set_visible(False)
# ax.set_xlabel('Time')
# ax.set_ylabel('No. of trips')
# axes2.set_ylabel('Average Fare')
# plt.savefig("timeVcountVfare.png")
