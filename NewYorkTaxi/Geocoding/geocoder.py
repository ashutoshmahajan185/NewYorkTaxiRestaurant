from arcgis.gis import GIS
from arcgis.geocoding import geocode, reverse_geocode
from arcgis.geometry import Point

readFile = open("restaurantAddress.txt", "r")
writeFile = open("resLatLon.txt", "w+")
readLine = readFile.readlines()

gis = GIS()
count = 1
for line in readLine:
	print(str(count) + ". " + line)
	geocode_result = geocode(address = line, as_featureset = True)
	result = geocode_result.features[0].geometry
	writeFile.write(line.strip() + "," + str(result['y']) + "," + str(result['x']) + "\n")
	count += 1
print("done")
readFile.close()
writeFile.close()