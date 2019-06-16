New York Taxi Version #1
Ashutosh Mahajan (abm523)
Shubham Divekar (sjd451)

# Application does not contain a User Interface but rather studies the data.
# Actuations are presented as a run job of the file with screenshot of the processing shown
# Project is not compiled into a Jar file but rather only individual scala files
# .zip contains a folder called TrailCode.zip containing all the extra code we attempted


Application description:
The application mainly focuses on studying the data by drawing graphs based on Average Fare, Average Speed, Number of Trips per day. Alternate Restaurant suggestion is done based on the input from user.
The restaurant name is processed to find its location and then proximity from cluster centers is calculated. If the restaurant is very close to the cluster center (is located in a highly trafficked region) the application will suggest the user with alternate locations based on same cuisine. The resulting restaurants will then again be processed to check whether they are located in less trafficked area and would be returned to the user.

Files required:
1.	Cleaning.scala "Pre-processing" #NOT A PART OF GRADING MATERIAL
	Contains cleaning code

2.	KMeans.scala "Pre-processing"
	Contains code to calculate clusters

3.	PlotGraph.scala "Application processing"
	Contains code for processing data as required for plotting the graphs
	Avg Fare, Avg speed, Number of trips in a day.

4. 	FareEstimation.scala "Application processing"
	Contains code for estimation of fare using the random forest regression model

5.	RestaurantCleaning.scala "Pre-processing" #NOT A PART OF GRADING MATERIAL
	Contains code for cleaning of Restaurant Data. Generates files for geocoding.
	Geocoded data is then joined with the remainder of restaurant data to get usable data

6.	AlternateRestaurant.scala "Actuation Application processing"
	Contains code that takes user input (hardcoded for this assignment) and calculates a list of alternate
	restaurants as described above.

7.	timeVfare.py "Visualization processing"
	Contains code that takes as input data frames processed/created in File#3 and generates graphs as described above

8.	HeatMap.py "Visualization processing"
	Contains code to plot cluster centers on a map of NYC

9.	geoCoder.py "pre-processing"
	Contains code for geocoding the restaurant address into Latitudes and Longitudes.
			
Running the application:
#	All the pre-processing files are run on Spark-shell using HDFS.
	=> Command: " :load(<filename>.scala) "
#	Visualization files are run on local workstation having python
	=> Command: " python <filename>.py "
#	Actuation Application processing file(File#6) can be run on local workstation having sbt installed.
	Running this file will need user to input restaurant name. The entire restaurant dataset contains about 25000 restaurant values. Four filters are applied 1. Cuisine, 2. Distance from cluster,
	3. Distance from user, 4. Fare
	Total number of Cuisines are 84, thus number of restaurants in each partition would be in an approximate range of 297 (not distibuted equally)
	The final value of suggested restaurants might vary vastly based on the combination of the values of filters applied.

Requirements for input files:
#	Paths are included in all files that are stored on HDFS
#	Paths can be set for files that are stored on Local workstation
#	Files required to be moved from HDFS to Local workstation (used for visualization)
	=> part-00000 file for clusters after KMeans clustering (Input for visualization and running File#6)
	=> Restaurant Data file (Input for running File#6)
	=> Cuisine Data file (Input for running File#6)
	=> Graph Data files (Input for visualization)

Output Files included (Screenshots and Visualizations)
SS1.	Avg Speed vs. Avg Fare vs. Number of Trips during a day
SS2.	Heatmap using Tableau
SS3.	Heatmap using python
SS4.	Output of fare prediction
SS5.	Suggested alternate restaurants for "MERCI MARKET" based on cuisine, user distance and cluster density filters
SS6.	Plot using Tableau for SS5


Explanation for insights:
ScreenShot1 (SS1): Here we can see the distribution of trip data; more number of trips <=> lower speed
			Higher demand <=> lower average fare (owing to increased number of trips)
			
ScreenShot5 (SS5): Depicts the list of restaurants that were suggested as an alternate for given input "MERCI MARKET"
ScreenShot6 (SS6): Depicts the restaurants distributed across the map of NYC. Red represents the user Location. Green denotes MERCI MARKET

ScreenShot4 (SS4 (x)): Depicts the processing of fare estimation using regression. Prediction shows the estimated value for given Label

