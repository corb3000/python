# based on UK gridded population 2011 based on Census 2011 and Land Cover Map 2015
# From here:
# https://www.data.gov.uk/dataset/ca2daae8-8f36-4279-b15d-78b0463c61db/uk-gridded-population-2011-based-on-census-2011-and-land-cover-map-2015


import asyncio
import json
from typing import List, TypedDict
from httpx import AsyncClient, Response
import jmespath
from typing_extensions import TypedDict
import sqlite3
import time
import numpy as np
import matplotlib.pyplot as plt
import re
from matplotlib.colors import ListedColormap, LinearSegmentedColormap
import matplotlib as mpl
import math
from OSGridConverter import latlong2grid

def value(txt):
    return int("".join(filter(str.isdigit, txt)))


def import_population():
    data = open('UK_residential_population_2011_1_km.asc')
    lines = data.readlines()
    population = {
        "ncols": value(lines[0]),
        "nrows": value(lines[1]),
        "xllcorner": value(lines[2]),
        "yllcorner": value(lines[3]),
        "cellsize": value(lines[4]),
    }
    print(population["ncols"])
    myArray = np.empty((0, population["ncols"]), int)

    for line in reversed(lines):
        if len(line) >100:
            myArray = np.vstack((myArray, np.fromstring(line, dtype = int, sep = ' ')))
    population["array"] = myArray
    return population

def averageArray(arr):
    i = 0
    c = 0
    for x in np.nditer(arr):
        if x > 0:
            c += x
        i += 1
    avg = c/i
    print(i, c, avg)
    return avg


def  find_area(myArray):

    for count, column in enumerate(myArray[1]):
        if column > -1:
            print(count, column)


def color_map():
    cmap = ListedColormap(["grey","darkorange", "gold", "lawngreen", "lightseagreen","blue","blueviolet","violet","fuchsia","pink","red"])
    
    return cmap

def plot_array(myArray):
    colormaps = color_map()
    fig, ax = plt.subplots(figsize=(11, 16),dpi = 100)
    psm = ax.pcolormesh(myArray["array"], cmap=colormaps, rasterized=True, vmin=-50, vmax=499)
    fig.colorbar(psm, ax=ax)
    plt.show()

def plot_Part_array(pop, lat, long, size):
    x, y = geoToGrid(long, lat)

    print(long, lat, x, y)
    a = max(x - size, 0)
    b = min(x + size + 1, pop["ncols"])
    c = max(y - size, 0)
    d = min(y + size + 1, pop["nrows"])
    myArray = pop['array']

    smallArray = myArray[c:d, a:b]
    print(a,b,c,d)
    colormaps = color_map()
    fig, ax = plt.subplots(figsize=(10, 10),dpi = 100)
    psm = ax.pcolormesh(smallArray, cmap=colormaps, rasterized=True, vmin=-50, vmax=499)
    fig.colorbar(psm, ax=ax)
    plt.show()

def population_density(pop, lat, long, size):
    x, y = geoToGrid(long, lat)
    print(long, lat, x, y)
    a = max(x - size, 0)
    b = min(x + size + 1, pop["ncols"])
    c = max(y - size, 0)
    d = min(y + size + 1, pop["nrows"])
    myArray = pop['array']
    smallArray = myArray[c:d, a:b]
    print(a,b,c,d)
    return int(averageArray(smallArray))

def geoToGrid(long, lat):
    g = latlong2grid(lat, long, tag = "WGS84")
    return int(g.E/1000-4), int(g.N/1000-7)

# def geoToGrid(long, lat):
    
#     latToKm50 = 111.2253
#     deltaLat = 0.01866666667
#     longToKm = 111.4 #was 111.46
#     bottomLat = 49.945 # was 49.88
#     commonLong = -2.99 # middle of map in longitude
#     refX = 326 
#     y = int((lat - bottomLat) * ((lat -50) * deltaLat + latToKm50))
#     x = int((long - commonLong) * longToKm * (math.cos(math.radians(lat))) + refX)
#     return x, y



def main(args=None):
    population = import_population()
    # plot_array(population)

    # plot_Part_array(population, 49.914456, -6.314041, 5) # Long, Lat, size
    # plot_Part_array(population, 51.387131, 1.432325, 10) #Margate y +1 
    # plot_Part_array(population, 49.970063, -5.202922, 5) #Lizard
    # plot_Part_array(population, 50.531564, -2.447630, 5) #Portland southwell x -2
    # plot_Part_array(population, 51.164880, -4.665621, 5) #Lundy x-2
    # plot_Part_array(population, 50.579036, -1.293321, 5) #IOW
    # plot_Part_array(population, 59.386760, -2.386772, 5) #Orknies y+1 x -2
    # plot_Part_array(population, 54.115594, -0.124127, 5) #flanborough Y+1 x -3
    # plot_Part_array(population, 52.758037, -4.787937, 5) # y+2 x -3



    conn = sqlite3.connect('houses.db')
    cur = conn.cursor()
    cur.execute("""SELECT * FROM house WHERE id = 134211254 """)
    rows = cur.fetchall()
    cur.execute("SELECT rowid, latitude, longitude FROM house")
    houses = cur.fetchall()

    
    for house in houses:
        density = []
        density.append(population_density(population, house[1], house[2], 0))
        density.append(population_density(population, house[1], house[2], 1))
        density.append(population_density(population, house[1], house[2], 3))
        density.append(house[0])
        data = tuple(density)
        cur.execute("UPDATE house SET density_1k = ?, density_3k = ?, density_5k = ? where rowid = ?", data)
    conn.commit()
  
    pass
    

if __name__ == '__main__':
    main()

