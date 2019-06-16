import gmplot 
import pandas as pd

df = pd.DataFrame()
df1 = pd.read_csv("KMeans500", header=None)
df1[0] = df1[0].apply(lambda x: float(x.strip("(([]))\n").strip(")")))
df1[1] = df1[1].apply(lambda x: float(x.strip("(([]))\n").strip(")")))
df = pd.concat([df1,df])

la=list(df[0].values)
lo=list(df[1].values)

gmap1 = gmplot.GoogleMapPlotter(40.751, -73.984,10)
gmap1.scatter( lo, la, '# FF0000', size = 20, marker = True)
gmap1.heatmap(lo, la)
gmap1.draw("heatmap500.html") 

print("Heat Map generated")


