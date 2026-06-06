"""Unit tests for the synthetic Spotify data generator (pure Python, no Spark)."""
import os
import sys
import random

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "data", "generator"))

from faker import Faker                                    # noqa: E402
from generate_spotify_data import (                        # noqa: E402
    generate_artists, generate_tracks, generate_plays,
)


def _seed():
    random.seed(42)
    Faker.seed(42)


def test_artist_count_and_unique_ids():
    _seed()
    artists = generate_artists(100)
    assert len(artists) == 100
    assert len({a["artist_id"] for a in artists}) == 100


def test_tracks_reference_real_artists():
    _seed()
    artist_ids = {a["artist_id"] for a in generate_artists(50)}
    tracks = generate_tracks(1000, list(artist_ids))
    for t in tracks:
        if t["artist_id"]:
            assert t["artist_id"] in artist_ids


def test_plays_have_two_percent_duplicates():
    _seed()
    plays = generate_plays(1000, ["trk_000001"])
    assert len(plays) == 1020


def test_reproducible_with_seed():
    _seed()
    first = generate_artists(20)
    _seed()
    second = generate_artists(20)
    assert first == second