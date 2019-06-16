import pandas as pd
import glob,os
import csv
import gmplot

print("Creating HTML file for clusters and stations ...")

df = pd.DataFrame()
df1 = pd.read_csv("part-00000", header=None)
df1[0] = df1[0].apply( lambda x: float(x.strip("(([]))\n").strip(")")))
df1[1] = df1[1].apply( lambda x: float(x.strip("(([]))\n").strip(")")))
df = pd.concat([df1,df])


la=list(df[0].values)

print(la)
lo=list(df[1].values)
data = zip(la,lo)
c_la,c_lon=zip(*data)   

not_worked = True
i = 0
# while(not_worked and i<100):
#     try:
#         i+=1
#         gmap = gmplot.GoogleMapPlotter.from_geocode(-74.006,40.712,20,apikey="AIzaSyCheyR059SCb2f9XQL-Yjd6R_J3mRi_Q_0")
#         not_worked = False
#         break
#     except:    
#         continue
gmap = gmplot.GoogleMapPlotter.from_geocode("New York City",apikey="AIzaSyCheyR059SCb2f9XQL-Yjd6R_J3mRi_Q_0")
for i in range(len(la)):
    gmap.marker(la[i],lo[i] , 'cornflowerblue')
gmap.draw("clusters_station.html")

print("Done!")