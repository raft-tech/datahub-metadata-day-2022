import unittest

from datahub.utilities.urns.error import InvalidUrnError
from datahub.utilities.urns.tag_urn import TagUrn


class TestTagUrn(unittest.TestCase):
    def test_parse_urn(self) -> None:
        tag_urn_str = "urn:li:tag:abc"
        tag_urn = TagUrn.create_from_string(tag_urn_str)
        assert tag_urn.get_type() == TagUrn.ENTITY_TYPE

        assert tag_urn.get_entity_id() == ["abc"]
        assert str(tag_urn) == tag_urn_str
        assert tag_urn == TagUrn("tag", ["abc"])
        assert tag_urn == TagUrn.create_from_id("abc")

    def test_invalid_urn(self) -> None:
        with self.assertRaises(InvalidUrnError):
            TagUrn.create_from_string("urn:li:abc:tag_id")

        with self.assertRaises(InvalidUrnError):
            TagUrn.create_from_string("urn:li:tag:(part1,part2)")
