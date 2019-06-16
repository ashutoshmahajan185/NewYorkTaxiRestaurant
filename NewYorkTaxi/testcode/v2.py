import pandas as pd
import glob,os
import csv
from tqdm import tqdm

#the data is copied to the local machine
#plot heatmap

print("Creating HTML file for heatmap ...")
df = pd.DataFrame()
##for i in tqdm(range(0,1763)):
df1 = pd.read_csv("part-00000", header=None)
df1[0] = df1[0].apply( lambda x: float(x.strip("(([]))\n").strip(")")))
df1[1] = df1[1].apply( lambda x: float(x.strip("(([]))\n").strip(")")))
df = pd.concat([df1,df])
    
la=list(df[0].values)
lo=list(df[1].values)
#la = [float(l) for l in la]
#lo = [float(l) for l in lo]
data = zip(la,lo)
las,lons=zip(*data)

from gmplot import gmplot
gmap = gmplot.GoogleMapPlotter.from_geocode("San Francisco")
gmplot.plot(37.428, -122.145)
        

#gmap.heatmap(las,lons)
gmap.draw("../heatmap.html")
     
print("Done!")
