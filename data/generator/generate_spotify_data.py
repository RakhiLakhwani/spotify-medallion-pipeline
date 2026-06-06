"""Generate synthetic Spotify-style data for the medallion pipeline.

Produces three related CSV files (artists, tracks, plays) with deliberately
injected data quality issues so downstream silver/DQ layers have something
real to catch. Reproducible and self-contained — no external API needed.
"""
import argparse
import csv
import random
import os
from faker import Faker

fake = Faker()
GENRES = ["pop", "rock", "hip-hop", "jazz", "electronic", "classical", "r&b", "country"]

def generate_artists(n: int) -> list[dict]:
    """Return n artist records: artist_id, name, genre, country.
    TODO: build clean artist rows. artist_id should be unique (e.g. 'art_00001').
    """
    artists = []
    for i in range(n):
        artists.append({"artist_id": f"art_{i:05d}",
                      "name": fake.name(),
                      "genre": random.choice(GENRES),
                      "country": fake.country(),
                      })
        
    return artists


def generate_tracks(n: int, artist_ids: list[str]) -> list[dict]:
    """Return n track records linked to existing artist_ids.
    Fields: track_id, artist_id, title, duration_ms, popularity, release_date.
    TODO: inject quality issues here ->
      - some rows with null/empty artist_id
      - a few negative duration_ms
      - occasional popularity > 100 (valid range is 0-100)
    """
    tracks = []
    for i in range(n):
        if random.random()<0.01:
            artist_id = ""
        else:
            artist_id = random.choice(artist_ids)

        if random.random()<0.005:
            duration_ms = -1*random.randint(1000,5000)
        
        else:
            duration_ms = 1*random.randint(60_000, 300_000)

        # ~1% get an out-of-range popularity (valid domain is 0–100).
        if random.random() < 0.01:
            popularity = random.randint(101, 150)
        
        else:
            popularity = random.randint(0,100)

        tracks.append({
            "track_id": f"trk_{i:06d}",
            "artist_id": artist_id,
            "title": fake.sentence(nb_words=3).rstrip("."),
            "duration_ms": duration_ms,
            "popularity":popularity,
            "release_date": fake.date_between(start_date= "-10y")
        })
    return tracks


def generate_plays(n: int, track_ids: list[str]) -> list[dict]:
    """Return n play events: play_id, track_id, user_id, played_at, ms_played.
    TODO: inject quality issues here ->
      - ~2% exact duplicate rows
      - a handful of malformed played_at timestamps (e.g. 'not_a_date')
    """
    plays = []
    for i in range(n):
        if random.random() < 0.05:
            played_at = "not_a_date"
        
        else:
            played_at = fake.date_time_this_year().isoformat()
    
        plays.append({
                "play_id": f"ply_{i:08d}",
                "track_id": random.choice(track_ids),
                "user_id": f"usr_{random.randint(1, 10_000):05d}",
                "played_at": played_at,
                "ms_played": random.randint(0, 300_000),
            })
    
    num_dupes = int(n * 0.02)
    if num_dupes and plays:
        plays.extend(random.sample(plays, num_dupes))

    return plays    



def write_csv(rows: list[dict], path: str) -> None:
    """Write a list of dicts to CSV at `path`, creating parent dirs if needed.

    Guards against an empty `rows` list (can't infer a header from nothing),
    so the script never crashes on an empty batch.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--artists", type=int, default=500)
    parser.add_argument("--tracks", type=int, default=5000)
    parser.add_argument("--plays", type=int, default=100000)
    parser.add_argument("--out", default="data/output")
    args = parser.parse_args()
    # TODO: call generators in dependency order (artists -> tracks -> plays),
    #       collect the id lists to pass downstream, then write each CSV.
    random.seed(42)
    Faker.seed(42)

    # Generate in dependency order and capture IDs to wire the tables together.
    artists = generate_artists(args.artists)
    artist_ids = [a["artist_id"] for a in artists]

    tracks = generate_tracks(args.tracks, artist_ids)
    track_ids = [t["track_id"] for t in tracks]

    plays = generate_plays(args.plays, track_ids)

    write_csv(artists, f"{args.out}/artists.csv")
    write_csv(tracks, f"{args.out}/tracks.csv")
    write_csv(plays, f"{args.out}/plays.csv")

    # Cheap observability: confirm the run and the volumes produced.
    print(
        f"Wrote {len(artists)} artists, {len(tracks)} tracks, "
        f"{len(plays)} plays to {args.out}/"
    )


if __name__ == "__main__":
    main()