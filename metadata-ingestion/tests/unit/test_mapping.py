from typing import Any, Dict

from datahub.metadata.com.linkedin.pegasus2avro.common import GlobalTags
from datahub.metadata.schema_classes import (
    GlossaryTermsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipSourceTypeClass,
)
from datahub.utilities.mapping import OperationProcessor


def get_operation_defs() -> Dict[str, Any]:
    return {
        "user_owner": {
            "match": ".*",
            "operation": "add_owner",
            "config": {"owner_type": "user"},
        },
        "user_owner_2": {
            "match": ".*",
            "operation": "add_owner",
            "config": {"owner_type": "user"},
        },
        "group.owner": {
            "match": ".*",
            "operation": "add_owner",
            "config": {"owner_type": "group"},
        },
        "pii": {
            "match": True,
            "operation": "add_tag",
            "config": {"tag": "has_pii_test"},
        },
        "int_property": {
            "match": 1,
            "operation": "add_tag",
            "config": {"tag": "int_property"},
        },
        "double_property": {
            "match": 2.5,
            "operation": "add_term",
            "config": {"term": "double_property"},
        },
        "governance.team_owner": {
            "match": "Finan.*",
            "operation": "add_term",
            "config": {"term": "Finance.test"},
        },
        "tag": {
            "match": ".*",
            "operation": "add_tag",
            "config": {"tag": "{{ $match }}"},
        },
    }


def test_operation_processor_not_matching():
    # no property matches to the rules
    raw_props = {
        "user_owner_test": "test_user@abc.com",
        "group.owner_test": "test.group@abc.co.in",
        "governance.team_owner": "Binance",
        "pii": False,
        "int_property": 3,
        "double_property": 25,
    }
    processor = OperationProcessor(get_operation_defs())
    aspect_map = processor.process(raw_props)
    assert "add_tag" not in aspect_map
    assert "add_term" not in aspect_map
    assert "add_owner" not in aspect_map


def test_operation_processor_matching():
    raw_props = {
        "user_owner": "test_user@abc.com",
        "user_owner_2": "test_user_2",
        "group.owner": "test.group@abc.co.in",
        "governance.team_owner": "Finance",
        "pii": True,
        "int_property": 1,
        "double_property": 2.5,
        "tag": "Finance",
    }
    processor = OperationProcessor(
        operation_defs=get_operation_defs(),
        owner_source_type="SOURCE_CONTROL",
        strip_owner_email_id=True,
    )
    aspect_map = processor.process(raw_props)
    assert "add_tag" in aspect_map
    assert "add_term" in aspect_map
    assert "add_owner" in aspect_map
    tag_aspect: GlobalTags = aspect_map["add_tag"]
    tags_added = [
        tag_association_class.tag for tag_association_class in tag_aspect.tags
    ]
    term_aspect: GlossaryTermsClass = aspect_map["add_term"]
    terms_added = [
        term_association_class.urn for term_association_class in term_aspect.terms
    ]
    assert (
        len(tags_added) == 3
        and "urn:li:tag:has_pii_test" in tags_added
        and "urn:li:tag:int_property" in tags_added
        and "urn:li:tag:Finance" in tags_added
    )
    assert (
        len(terms_added) == 2
        and "urn:li:glossaryTerm:Finance.test" in terms_added
        and "urn:li:glossaryTerm:double_property" in terms_added
    )

    ownership_aspect: OwnershipClass = aspect_map["add_owner"]
    assert len(ownership_aspect.owners) == 3
    owner_set = {
        "urn:li:corpuser:test_user",
        "urn:li:corpuser:test_user_2",
        "urn:li:corpGroup:test.group",
    }
    for single_owner in ownership_aspect.owners:
        assert single_owner.owner in owner_set
        assert (
            single_owner.source
            and single_owner.source.type == OwnershipSourceTypeClass.SOURCE_CONTROL
        )


def test_operation_processor_no_email_strip_source_type_not_null():
    raw_props = {
        "user_owner": "test_user@abc.com",
    }
    processor = OperationProcessor(
        operation_defs=get_operation_defs(),
        owner_source_type="SERVICE",
        strip_owner_email_id=False,
    )
    aspect_map = processor.process(raw_props)
    assert "add_owner" in aspect_map

    ownership_aspect: OwnershipClass = aspect_map["add_owner"]
    assert len(ownership_aspect.owners) == 1
    new_owner: OwnerClass = ownership_aspect.owners[0]
    assert new_owner.owner == "urn:li:corpuser:test_user@abc.com"
    assert new_owner.source and new_owner.source.type == "SERVICE"
