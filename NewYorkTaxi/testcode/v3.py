# import gmplot package 
import gmplot 
import pandas as pd
# GoogleMapPlotter return Map object 
# Pass the center latitude and 
# center longitude 
  
# Pass the absolute path 
df = pd.DataFrame()
df1 = pd.read_csv("part-00000", header=None)
df1[0] = df1[0].apply( lambda x: float(x.strip("(([]))\n").strip(")")))
df1[1] = df1[1].apply( lambda x: float(x.strip("(([]))\n").strip(")")))
df = pd.concat([df1,df])
la=list(df[0].values)
lo=list(df[1].values)

df = pd.DataFrame()
df1 = pd.read_csv("part-00001", header=None)
df1[0] = df1[0].apply( lambda x: float(x.strip("(([]))\n").strip(")")))
df1[1] = df1[1].apply( lambda x: float(x.strip("(([]))\n").strip(")")))
df = pd.concat([df1,df])
la1=list(df[0].values)
lo1=list(df[1].values)

gmap1 = gmplot.GoogleMapPlotter(40.751,-73.984,10)
# gmap1.scatter( lo1, la1, '# FF0000', 
#                               size = 20, marker = False )

gmap1.heatmap( lo1, la1)

gmap1.draw( "map11.html" ) 


