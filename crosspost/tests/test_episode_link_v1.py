"""SCHEDULED_EPISODE_LINK_V1 guard."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from transformers.caption import build_caption, mentions_reply_link
from monitors.x_reply_link import rumble_links_from_detail

TXT = ("‘Trump’s alignment with Putin is out of fear of China🇨🇳 and an attempt to create a new US🇺🇸-Russia🇷🇺 axis.’\n\n"
       "—John Perkins, Former Economic Hitman joins us for Saturday’s episode of Going Underground.\n\n"
       "Don’t miss it, follow us on X and follow our Rumble channel, link below in the replies 👇\n\n@jperkinsauthor")


def test_caption():
    post = {"text": TXT, "handle": "Gunderground_tv", "tweet_id": "1"}
    assert mentions_reply_link(TXT)
    c = build_caption(post, "facebook_profile" if False else None)
    assert "link below in the replies" not in c and "Perkins" in c and "rumble.com/c/GoingUnderground" in c
    assert "Don’t miss it, follow us on X and follow our Rumble channel, link below 👇" in c
    post["episode_url"] = "https://rumble.com/v7abcde-john-perkins-trump-putin.html"
    c = build_caption(post)
    assert "rumble.com/v7abcde-john-perkins-trump-putin.html" in c and "rumble.com/c/GoingUnderground" not in c


def test_links_only_from_author():
    def node(rid, uid, reply_to, url):
        return {"rest_id": rid, "legacy": {"user_id_str": uid, "in_reply_to_status_id_str": reply_to,
                                            "entities": {"urls": [{"expanded_url": url}] if url else []}, "full_text": ""}}
    pl = {"x": [node("100", "GU", None, None),
                node("101", "OTHER", "100", "https://rumble.com/v9spam-x.html"),
                node("102", "GU", "100", "https://rumble.com/v7abcde-john-perkins.html")]}
    assert rumble_links_from_detail([pl], "100") == ["https://rumble.com/v7abcde-john-perkins.html"]
    pl2 = {"x": [node("100", "GU", None, None), node("101", "OTHER", "100", "https://rumble.com/v9spam-x.html")]}
    assert rumble_links_from_detail([pl2], "100") == []


if __name__ == "__main__":
    test_caption(); test_links_only_from_author(); print("PASS")
