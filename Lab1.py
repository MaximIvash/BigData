import csv
from collections import defaultdict

def mapper(file_path):
    mapped = []

    with open(file_path, encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                location_id = row["PULocationID"]
                mapped.append((location_id, 1))
            except KeyError:
                continue

    return mapped

def shuffle(mapped_data):
    shuffled = defaultdict(list)

    for key, value in mapped_data:
        shuffled[key].append(value)

    return shuffled


def reducer(shuffled_data):
    reduced = {}

    for key, values in shuffled_data.items():
        reduced[key] = sum(values)

    return reduced

if __name__ == "__main__":
    file_path = "nyc_yellow_taxi_trip_records_from_Jan_to_Aug_2023.csv"

    mapped = mapper(file_path)

    shuffled = shuffle(mapped)

    reduced = reducer(shuffled)

    ranked_zones = sorted(
        reduced.items(),
        key=lambda x: x[1],
        reverse=True
    )

    print("\nТоп-10 самых популярных зон посадки такси:")
    for zone, trips in ranked_zones[:10]:
        print(f"Зона {zone}: {trips} поездок")
