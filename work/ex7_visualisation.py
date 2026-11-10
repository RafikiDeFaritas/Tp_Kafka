import json
from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd
from hdfs import InsecureClient

hdfs_client = InsecureClient("http://namenode:9870", user="root")

records = []
for root, _, files in hdfs_client.walk("/hdfs-data"):
    for file in files:
        if file == "alerts.json":
            with hdfs_client.read(f"{root}/{file}", encoding="utf-8") as reader:
                for line in reader:
                    if line.strip():
                        records.append(json.loads(line))

df = pd.DataFrame(records)
df["event_time"] = pd.to_datetime(df["event_time"])
df = df.sort_values("event_time")

fig, axes = plt.subplots(2, 2, figsize=(14, 10))

# 1. Évolution de la température au fil du temps
for city, group in df.groupby("city"):
    axes[0, 0].plot(group["event_time"], group["temperature"], marker="o", label=city)
axes[0, 0].set_title("Évolution de la température (°C)")
axes[0, 0].legend()
axes[0, 0].tick_params(axis="x", rotation=30)

# 2. Évolution de la vitesse du vent
for city, group in df.groupby("city"):
    axes[0, 1].plot(group["event_time"], group["windspeed"], marker="o", label=city)
axes[0, 1].set_title("Évolution de la vitesse du vent (m/s)")
axes[0, 1].legend()
axes[0, 1].tick_params(axis="x", rotation=30)

# 3. Nombre d'alertes vent et chaleur par niveau
levels = ["level_0", "level_1", "level_2"]
alert_counts = pd.DataFrame({
    "Vent": df["wind_alert_level"].value_counts().reindex(levels, fill_value=0),
    "Chaleur": df["heat_alert_level"].value_counts().reindex(levels, fill_value=0),
})
alert_counts.plot(kind="bar", ax=axes[1, 0], rot=0)
axes[1, 0].set_title("Nombre d'alertes vent et chaleur par niveau")

# 4. Code météo le plus fréquent par pays
freq_code = df.groupby("country")["weather_code"].agg(lambda x: x.mode().iloc[0])
freq_code.plot(kind="bar", ax=axes[1, 1], color="steelblue", rot=20)
axes[1, 1].set_title("Code météo le plus fréquent par pays")
axes[1, 1].set_ylabel("weather_code")
for i, val in enumerate(freq_code):
    axes[1, 1].text(i, val, str(val), ha="center", va="bottom")

plt.tight_layout()
out_path = Path(__file__).with_name("visualisations_meteo.png")
plt.savefig(out_path)
print(f"Graphiques sauvegardés dans {out_path}")
print("\nCode météo le plus fréquent par pays :")
print(freq_code)
plt.show()
