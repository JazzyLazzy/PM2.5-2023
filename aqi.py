import json
import pandas as pd
import sqlite3
from scipy.spatial import cKDTree
import numpy as np

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0


    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad


    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance = R * c
    return distance


with open('output.json', 'r') as file:
    data = json.load(file)

#Sometimes some requests fail so multiple jsons were created with the failed ones
with open('failedoutput.json', 'r') as file:
    data1 = json.load(file)


with open('failedoutput1.json', 'r') as file:
    data2 = json.load(file)


unhealthy_threshold = 35
#combining all jsons
df = pd.DataFrame(data + data1 + data2)
print(df.head())
grouped = df.groupby(['latitude', 'longitude']).agg(
    average_arithmetic_mean=('arithmetic_mean', 'mean'),
    unhealthy_count=('arithmetic_mean', lambda x: (x > unhealthy_threshold).sum())
).reset_index()


print(grouped.head())

points = grouped[['latitude', 'longitude']].to_numpy()

tree = cKDTree(points)

inx = []

for p in points:
    inx.append(tree.query_ball_point(p, .4))

size = np.zeros((len(points), 2))

for i in range(len(inx)):
    size[i][0] = i
    size[i][1] = len(inx[i])

sorted = size[np.argsort(size[:, 1])]
sorted = sorted[::-1]
l = set()
end = []
for s in sorted:
    if not (s[0] in l):
        avg_con = 0
        avg_days = 0
        tot = 0
        for t in inx[int(s[0])]:
            if not (t in l):
                avg_con = avg_con + grouped["average_arithmetic_mean"].iloc[t]
                avg_days = avg_days + grouped["unhealthy_count"].iloc[t]
                tot = tot + 1
                l.add(t)
        avg_con = avg_con / tot
        avg_days = avg_days / tot
        end.append({'latitude': points[int(s[0])][0], 'longitude': points[int(s[0])][1], 'average_arithmetic_mean': avg_con, 'unhealthy_count': avg_days, 'combined': tot})

enddf = pd.DataFrame(end);
print(enddf.head())

conn = sqlite3.connect('my_database.db')
grouped.to_sql('air_quality', conn, if_exists='replace', index=False)
enddf.to_sql('air_quality_comb', conn, if_exists='replace', index=False)
conn.close()
