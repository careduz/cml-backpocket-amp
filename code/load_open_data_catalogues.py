import os
from apache_atlas.client.base_client import AtlasClient
from apache_atlas.model.instance import (
    AtlasEntity,
    AtlasEntityWithExtInfo,
    AtlasRelatedObjectId,
    AtlasClassification,
    AtlasStruct,
)
from apache_atlas.model.typedef import (
    AtlasEntityDef,
    AtlasEntityDef,
    AtlasAttributeDef,
    AtlasRelationshipDef,
    AtlasStructDef,
    AtlasClassificationDef,
)
from dateutil.parser import parse
import requests
import json
import logging

datalake_atlas_endpoint = os.environ["DATALAKE_ATLAS_ENDPOINT"]
if datalake_atlas_endpoint.endswith("/"):
    datalake_atlas_endpoint = datalake_atlas_endpoint[
        0 : len(datalake_atlas_endpoint) - 1
    ]

client = AtlasClient(
    f"{datalake_atlas_endpoint}/v2",
    (os.environ["PROJECT_OWNER"], os.environ["WORKLOAD_PASSWORD"]),
)

type_name_ckan = "ckan"
type_name_ckan_site = f"{type_name_ckan}_site"
type_name_ckan_licenses = f"{type_name_ckan}_license"
type_name_ckan_organization = f"{type_name_ckan}_organization"
type_name_ckan_package = f"{type_name_ckan}_package"
type_name_ckan_resource = f"{type_name_ckan}_resource"
type_name_ckan_datastore_resource = f"{type_name_ckan}_datastore_resource"
type_name_rlship_site_org = f"{type_name_ckan}_site_to_organizations"
type_name_rlship_org_packages = f"{type_name_ckan}_organization_to_packages"
type_name_rlship_package_resources = f"{type_name_ckan}_package_to_resources"
type_name_rlship_package_datastore_resources = (
    f"{type_name_ckan}_package_to_datastore_resources"
)
type_name_rlship_package_license = f"{type_name_ckan}_package_to_license"
type_name_struct_datastore_schema = f"{type_name_ckan}_datastore_resource_schema"
type_name_struct_resource_cache = f"{type_name_ckan}_datastore_cache_resource"


def get_packages(api_url: str):
    processed = 0
    count = 0
    catalogue = []

    logging.info(f"Getting CKAN packages...")
    while not processed or processed < count:
        res = requests.get(
            f"{api_url}package_search?rows=1000&start={processed}"
        ).json()["result"]
        count = res.pop("count")
        processed += len(res["results"])
        catalogue.extend(res["results"])
        logging.info(f"CKAN packages: {processed}/{count}")

    return catalogue


def get_toronto_topics(api_url=None, catalogue=None):
    if catalogue is None:
        catalogue = requests.get(api_url + f"package_search?rows=1000").json()[
            "result"
        ]["results"]

    topics = []
    for p in catalogue:
        if "topics" not in p:
            continue

        topics.extend(p["topics"].lower().split(","))

    return list(set(topics))


def get_ontario_keywords(api_url=None, catalogue=None):
    if catalogue is None:
        catalogue = get_packages(api_url)

    count = {}
    for p in catalogue:
        if not p.get("keywords") or not p["keywords"].get("en"):
            continue

        keywords = [x.lower() for x in p["keywords"]["en"]]
        keywords = ["health" if "health" in x or "covid" in x else x for x in keywords]
        for k in keywords:
            if count.get(k) is None:
                count[k] = 0

            count[k] += 1

    count = {k: v for k, v in count.items() if v > 5}

    return list(count.keys()), count


def make_typedefs(atlas_client, create_or_update="update", tags=[]):
    service_type = "ckan"
    datastore_resource_schema_struct_def = AtlasStructDef(
        {
            "name": type_name_struct_datastore_schema,
            "description": "ckan_datastore_resource_schema",
            "serviceType": service_type,
            "attributeDefs": [
                AtlasAttributeDef(
                    {
                        "name": "name",
                        "typeName": "string",
                        "isUnique": False,
                        "isOptional": False,
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "description",
                        "typeName": "string",
                        "isUnique": False,
                        "isOptional": True,
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "data_type",
                        "typeName": "string",
                        "isOptional": False,
                        "isUnique": False,
                        "cardinality": "SINGLE",
                    }
                ),
            ],
        }
    )

    datastore_cached_resource_struct_def = AtlasStructDef(
        {
            "name": type_name_struct_resource_cache,
            "description": "ckan_datastore_cached",
            "serviceType": service_type,
            "attributeDefs": [
                AtlasAttributeDef(
                    {
                        "name": "format",
                        "typeName": "string",
                        "isUnique": False,
                        "isOptional": False,
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "url",
                        "typeName": "string",
                        "isUnique": False,
                        "isOptional": False,
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "ckan_id",
                        "typeName": "string",
                        "isOptional": False,
                        "isUnique": False,
                        "cardinality": "SINGLE",
                    }
                ),
            ],
        }
    )

    ckan_entity_def = AtlasEntityDef(
        {
            "name": type_name_ckan,
            "description": "CKAN site containing the catalogue",
            "serviceType": service_type,
            "superTypes": ["Asset"],
            "attributeDefs": [
                AtlasAttributeDef(
                    {
                        "name": "ckan_site_name",
                        "typeName": "string",
                        "isUnique": False,
                        "isOptional": False,
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "ckan_id",
                        "typeName": "string",
                        "isUnique": False,
                        "isOptional": True,
                        "cardinality": "SINGLE",
                    }
                ),
            ],
        }
    )

    license_entity_def = AtlasEntityDef(
        {
            "name": type_name_ckan_licenses,
            "description": "License",
            "serviceType": service_type,
            "superTypes": [ckan_entity_def.name],
            "attributeDefs": [
                AtlasAttributeDef(
                    {
                        "name": "url",
                        "typeName": "string",
                        "isUnique": False,
                        "isOptional": True,
                        "cardinality": "SINGLE",
                    }
                ),
            ],
        }
    )

    site_entity_def = AtlasEntityDef(
        {
            "name": type_name_ckan_site,
            "description": "CKAN site containing the catalogue",
            "serviceType": service_type,
            "superTypes": [ckan_entity_def.name],
            "attributeDefs": [
                AtlasAttributeDef(
                    {
                        "name": "api_url",
                        "typeName": "string",
                        "description": "URL to root CKAN api of site, including /api/3/",
                        "isUnique": False,
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "site_url",
                        "typeName": "string",
                        "isUnique": False,
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "portal_url",
                        "typeName": "string",
                        "isUnique": False,
                        "isOptional": True,
                        "cardinality": "SINGLE",
                    }
                ),
            ],
        }
    )

    org_entity_def = AtlasEntityDef(
        {
            "name": type_name_ckan_organization,
            "description": type_name_ckan_organization,
            "serviceType": service_type,
            "superTypes": [ckan_entity_def.name],
            "attributeDefs": [
                AtlasAttributeDef(
                    {
                        "name": "title",
                        "typeName": "string",
                        "isUnique": False,
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "approval_status",
                        "typeName": "string",
                        "isUnique": False,
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "state",
                        "typeName": "string",
                        "isUnique": False,
                        "cardinality": "SINGLE",
                    }
                ),
            ],
        }
    )

    package_entity_def = AtlasEntityDef(
        {
            "name": type_name_ckan_package,
            "description": type_name_ckan_package,
            "serviceType": service_type,
            "superTypes": [ckan_entity_def.name],
            "attributeDefs": [
                AtlasAttributeDef(
                    {
                        "name": "title",
                        "typeName": "string",
                        "isUnique": False,
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "maintainer_email",
                        "typeName": "string",
                        "isOptional": True,
                        "isUnique": False,
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "mantainer",
                        "typeName": "string",
                        "isUnique": False,
                        "isOptional": True,
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "owner_division",
                        "typeName": "string",
                        "isUnique": False,
                        "isOptional": True,
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "owner_email",
                        "typeName": "string",
                        "isUnique": False,
                        "isOptional": True,
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "author",
                        "typeName": "string",
                        "isUnique": False,
                        "isOptional": True,
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "author_email",
                        "typeName": "string",
                        "isUnique": False,
                        "isOptional": True,
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "information_url",
                        "typeName": "string",
                        "isUnique": False,
                        "isOptional": True,
                        "description": "where to go for more information",
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "date_published",
                        "typeName": "date",
                        "isUnique": False,
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "refresh_rate",
                        "typeName": "string",
                        "isUnique": False,
                        "isOptional": True,
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "url",
                        "typeName": "string",
                        "isUnique": False,
                        "isOptional": False,
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "notes",
                        "typeName": "string",
                        "isUnique": False,
                        "isOptional": True,
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "limitations",
                        "typeName": "string",
                        "isUnique": False,
                        "isOptional": True,
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "is_retired",
                        "typeName": "boolean",
                        "isUnique": False,
                        "isOptional": True,
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "license_title",
                        "typeName": "string",
                        "isUnique": False,
                        "isOptional": True,
                        "cardinality": "SINGLE",
                    }
                ),
            ],
        }
    )

    resource_entity_def = AtlasEntityDef(
        {
            "name": type_name_ckan_resource,
            "description": type_name_ckan_resource,
            "serviceType": service_type,
            "superTypes": [ckan_entity_def.name],
            "attributeDefs": [
                AtlasAttributeDef(
                    {
                        "name": "format",
                        "typeName": "string",
                        "isOptional": False,
                        "isUnique": False,
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "url",
                        "typeName": "string",
                        "isUnique": False,
                        "isOptional": False,
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "datastore_active",
                        "typeName": "boolean",
                        "isUnique": False,
                        "isOptional": False,
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "last_modified",
                        "typeName": "date",
                        "isOptional": False,
                        "isUnique": False,
                        "cardinality": "SINGLE",
                    }
                ),
            ],
        }
    )

    datastore_resource_entity_def = AtlasEntityDef(
        {
            "name": type_name_ckan_datastore_resource,
            "serviceType": service_type,
            "description": "Datastore resource from CKAN, uncluding schema and resource cache",
            "superTypes": [resource_entity_def.name],
            "attributeDefs": [
                AtlasAttributeDef(
                    {
                        "name": "data_dictionary",
                        "typeName": f"array<{datastore_resource_schema_struct_def.name}>",
                        "isUnique": False,
                        "isOptional": False,
                        "cardinality": "SINGLE",
                    }
                ),
                AtlasAttributeDef(
                    {
                        "name": "cached_resources",
                        "typeName": f"array<{datastore_cached_resource_struct_def.name}>",
                        "isUnique": False,
                        "isOptional": True,
                        "cardinality": "SINGLE",
                    }
                ),
            ],
        }
    )

    site_org_rlship = AtlasRelationshipDef(
        {
            "name": type_name_rlship_site_org,
            "description": "CKAN site to organizations",
            "relationshipCategory": "COMPOSITION",
            "endDef1": {
                "type": site_entity_def.name,
                "name": "organizations",
                "isContainer": True,
                "cardinality": "SET",
                "isLegacyAttribute": False,
            },
            "endDef2": {
                "type": org_entity_def.name,
                "name": "site",
                "isContainer": False,
                "cardinality": "SINGLE",
                "isLegacyAttribute": False,
            },
        }
    )

    org_pkg_rlship = AtlasRelationshipDef(
        {
            "name": type_name_rlship_org_packages,
            "description": "CKAN organization to packages",
            "relationshipCategory": "COMPOSITION",
            "endDef1": {
                "type": org_entity_def.name,
                "name": "packages",
                "isContainer": True,
                "cardinality": "SET",
                "isLegacyAttribute": False,
            },
            "endDef2": {
                "type": package_entity_def.name,
                "name": "organization",
                "isContainer": False,
                "cardinality": "SINGLE",
                "isLegacyAttribute": False,
            },
        }
    )

    pkg_resc_rlship = AtlasRelationshipDef(
        {
            "name": type_name_rlship_package_resources,
            "description": "CKAN package to resources",
            "relationshipCategory": "COMPOSITION",
            "endDef1": {
                "type": package_entity_def.name,
                "name": "filestore_resources",
                "isContainer": True,
                "cardinality": "SET",
                "isLegacyAttribute": False,
            },
            "endDef2": {
                "type": resource_entity_def.name,
                "name": "package",
                "isContainer": False,
                "cardinality": "SINGLE",
                "isLegacyAttribute": False,
            },
        }
    )

    pkg_ds_resc_rlship = AtlasRelationshipDef(
        {
            "name": type_name_rlship_package_datastore_resources,
            "description": "CKAN package to datastore resources",
            "relationshipCategory": "COMPOSITION",
            "endDef1": {
                "type": package_entity_def.name,
                "name": "datastore_resources",
                "isContainer": True,
                "cardinality": "SET",
                "isLegacyAttribute": False,
            },
            "endDef2": {
                "type": datastore_resource_entity_def.name,
                "name": "package",
                "isContainer": False,
                "cardinality": "SINGLE",
                "isLegacyAttribute": False,
            },
        }
    )

    pkg_license_rlship = AtlasRelationshipDef(
        {
            "name": type_name_rlship_package_license,
            "description": "CKAN package to license",
            "relationshipCategory": "ASSOCIATION",
            "endDef1": {
                "type": package_entity_def.name,
                "name": "license",
                "isContainer": False,
                "cardinality": "SINGLE",
                "isLegacyAttribute": False,
            },
            "endDef2": {
                "type": license_entity_def.name,
                "name": "package",
                "isContainer": False,
                "cardinality": "SINGLE",
                "isLegacyAttribute": False,
            },
        }
    )

    classification_defs = [
        AtlasClassificationDef(
            {
                "name": "topic",
                "description": "Topic associated with dataset",
            }
        ),
        AtlasClassificationDef(
            {
                "name": "EXTERNAL",
                "description": "entity contained in external data source",
            }
        ),
        AtlasClassificationDef(
            {
                "name": "status",
                "description": "status of dataset",
            }
        ),
        AtlasClassificationDef(
            {
                "name": "RETIRED",
                "description": "Dataset is retired. Do not use, but if you must then let it be at your own risk.",
                "superTypes": ["status"],
            }
        ),
        AtlasClassificationDef(
            {
                "name": "ACTIVE",
                "description": "Dataset is active",
                "superTypes": ["status"],
            }
        ),
    ] + [
        AtlasClassificationDef(
            {
                "name": t,
                "description": f"Used to classify data that has do to with {t}",
                "superTypes": ["topic"],
            }
        )
        for t in tags
    ]

    typedefs = {
        "classificationDefs": classification_defs,
        "structDefs": [
            datastore_resource_schema_struct_def,
            datastore_cached_resource_struct_def,
        ],
        "entityDefs": [
            ckan_entity_def,
            site_entity_def,
            org_entity_def,
            package_entity_def,
            resource_entity_def,
            datastore_resource_entity_def,
            license_entity_def,
        ],
        "relationshipDefs": [
            site_org_rlship,
            org_pkg_rlship,
            pkg_resc_rlship,
            pkg_ds_resc_rlship,
            pkg_license_rlship,
        ],
    }

    assert create_or_update in ["create", "update"]

    if create_or_update == "create":
        return atlas_client.typedef.create_atlas_typedefs(typedefs)

    return atlas_client.typedef.update_atlas_typedefs(typedefs)


ontario_packages = get_packages("https://data.ontario.ca/api/3/action/")
toronto_packages = get_packages("https://open.toronto.ca/api/")

_topics = get_toronto_topics("https://open.toronto.ca/api/", toronto_packages)
_keywords, _ = get_ontario_keywords(
    "https://data.ontario.ca/api/3/action/", ontario_packages
)
tags = [t.lower().replace(" ", "_") for t in set(_topics + _keywords)]

make_typedefs(client, "create", tags)


def _convert_timestamp_to_epoch(t):
    return int(parse(t).strftime("%s")) * 1000


def _sanitized(s):
    if not s:
        return ""

    clean = "".join(
        e
        for e in s.replace("/n", " ").replace("/r", " ")
        if e.isalnum()
        or e
        in [
            " ",
            ".",
            "/",
            "[",
            "]",
            ":",
            "(",
            ")",
            ",",
            ";",
            "{",
            "}",
            "-",
            "_",
            "=",
            "+",
            "+",
            "$",
            "%",
            "*",
        ]
    )
    return " ".join([x for x in clean.split(" ") if len(x) > 0])


def _get_or_create_entity(
    type_name,
    qualified_name,
    entity_name,
    create_if_new,
    entity_attributes,
    relationship_attributes,
    classifications,
    atlas_client,
    update_if_exists,
):
    unique_attr_name = "qualifiedName"

    def _make_entity():
        entity = AtlasEntity({"typeName": type_name})
        entity.attributes = {
            k: None if v in ["", None] else v for k, v in entity_attributes.items()
        }
        entity.relationshipAttributes = relationship_attributes
        entity.classifications = classifications

        return AtlasEntityWithExtInfo({"entity": entity})

    prefix = f" | {type_name} | {entity_name} | {qualified_name}"

    try:
        entity = atlas_client.entity.get_entity_by_attribute(
            type_name=type_name, uniq_attributes=[(unique_attr_name, qualified_name)]
        )
        msg = "RETRIEVED" + prefix
        if update_if_exists:
            client.entity.update_entity(_make_entity())
            entity = atlas_client.entity.get_entity_by_attribute(
                type_name=type_name,
                uniq_attributes=[(unique_attr_name, qualified_name)],
            )
            msg = "UPDATED" + prefix
        logging.debug(msg)

        return entity.entity

    except Exception as e:
        err = str(e)
        if create_if_new is True:
            client.entity.create_entity(_make_entity())
            entity = atlas_client.entity.get_entity_by_attribute(
                type_name=type_name,
                uniq_attributes=[(unique_attr_name, qualified_name)],
            )
            msg = "CREATED" + prefix
            logging.debug(msg)
            return entity.entity

        raise e


def get_or_create_ckan_site_entity(
    name: str,
    api_url: str,
    site_url: str,
    qualified_name: str,
    owner: str,
    ckan_site_name: str,
    atlas_client,
    portal_url: str = None,
    type_name: str = type_name_ckan_site,
    description: str = None,
    update_if_exists=False,
):
    return _get_or_create_entity(
        type_name=type_name,
        qualified_name=qualified_name,
        entity_name=name,
        create_if_new=True,
        entity_attributes={
            "ckan_site_name": ckan_site_name,
            "name": name,
            "api_url": api_url,
            "site_url": site_url,
            "portal_url": portal_url,
            "description": description,
            "userDescription": description,
            "owner": owner,
            "qualifiedName": qualified_name,
        },
        atlas_client=atlas_client,
        update_if_exists=update_if_exists,
        relationship_attributes=None,
        classifications=None,
    )


def get_or_create_org_entity(
    title: str,
    name: str,
    approval_status: str,
    state: str,
    ckan_site_entity_guid: str,
    qualified_name: str,
    ckan_site_name: str,
    owner: str,
    atlas_client,
    description: str = None,
    type_name=type_name_ckan_organization,
    update_if_exists=False,
):
    return _get_or_create_entity(
        type_name=type_name,
        qualified_name=qualified_name,
        entity_name=name,
        create_if_new=True,
        entity_attributes={
            "ckan_site_name": ckan_site_name,
            "title": title,
            "name": name,
            "approval_status": approval_status,
            "state": state,
            "description": description,
            "owner": owner,
            "qualifiedName": qualified_name,
            "userDescription": description,
        },
        relationship_attributes={
            "site": AtlasRelatedObjectId({"guid": ckan_site_entity_guid}),
        },
        atlas_client=atlas_client,
        update_if_exists=update_if_exists,
        classifications=None,
    )


def _build_pkg_entity(
    title: str,
    ckan_site_name: str,
    name: str,
    date_published: str,
    refresh_rate: str,
    owner_division: str,
    open_data_portal_url: str,
    organization_entity_guid: str,
    qualified_name: str,
    owner: str,
    is_retired,
    notes: str,
    limitations: str,
    description: str,
    maintainer_email: str,
    owner_email: str,
    author: str,
    author_email: str,
    information_url: str,
    mantainer: str,
    type_name: str,
    topics: list[str],
    license_title: str,
    ckan_id: str,
):
    retired = str(is_retired).lower() == "true"

    return AtlasEntity(
        {
            "typeName": type_name,
            "attributes": {
                "ckan_site_name": ckan_site_name,
                "title": title,
                "description": _sanitized(description),
                "name": name,
                "date_published": _convert_timestamp_to_epoch(date_published),
                "mantainer": mantainer,
                "maintainer_email": maintainer_email,
                "owner_division": owner_division,
                "owner_email": owner_email,
                "refresh_rate": refresh_rate,
                "author": author,
                "notes": _sanitized(notes),
                "limitations": _sanitized(limitations),
                "author_email": author_email,
                "information_url": information_url,
                "url": open_data_portal_url,
                "owner": owner,
                "is_retired": retired,
                "license_title": license_title,
                "ckan_id": ckan_id,
                "qualifiedName": qualified_name,
                "userDescription": description,
            },
            "relationshipAttributes": {
                "organization": AtlasRelatedObjectId(
                    {"guid": organization_entity_guid}
                ),
            },
            "classifications": [
                AtlasClassification(
                    {
                        "typeName": "RETIRED" if retired else "ACTIVE",
                        "propagate": True,
                        "removePropagationsOnEntityDelete": True,
                    }
                ),
                AtlasClassification(
                    {
                        "typeName": "EXTERNAL",
                        "propagate": True,
                        "removePropagationsOnEntityDelete": True,
                    }
                ),
            ]
            + [
                AtlasClassification(
                    {
                        "typeName": t,
                        "propagate": True,
                        "removePropagationsOnEntityDelete": True,
                    }
                )
                for t in topics
            ]
            if topics
            else None,
        }
    )


def _prepare_package_topics(
    allowed_tags: list[str],
    topics=None,
    keywords={},
):
    try:
        topics_list = [] if not topics else topics.lower().replace(" ", "_").split(",")
        keywords_list = []
        if keywords and keywords.get("en"):
            eng = keywords.get("en")
            if eng:
                eng = [x.lower().replace(" ", "_") for x in eng]
                keywords_list.extend(
                    ["health" if "health" in t or "covid" in t else t for t in eng]
                )

        return [x for x in set(topics_list + keywords_list) if x in allowed_tags]

    except Exception as e:
        logging.error(f"topics: {json.dumps(topics)}")
        logging.error(f"keywords: {json.dumps(keywords)}")
        logging.error(f"allowed_tags: {json.dumps(allowed_tags)}")
        raise e


def get_or_create_pkg_entity(
    title: str,
    name: str,
    owner_division: str,
    open_data_portal_url: str,
    organization_entity_guid: str,
    org_qualified_name: str,
    owner: str,
    atlas_client,
    id: str,
    ckan_site_name: str,
    allowed_tags: list[str],
    refresh_rate: str = None,
    date_published: str = None,
    notes: str = None,
    limitations: str = None,
    excerpt: str = None,
    maintainer_email: str = None,
    owner_email: str = None,
    author: str = None,
    author_email: str = None,
    information_url: str = None,
    mantainer: str = None,
    type_name: str = type_name_ckan_package,
    topics: str = None,
    update_if_exists=False,
    is_retired=False,
    license_title=None,
    keywords=None,
    **kwargs,
):
    refresh_rate = refresh_rate.lower() if refresh_rate else None
    name = name if not id == name else title
    qualified_name = f"{name}.{org_qualified_name}"
    entity = _build_pkg_entity(
        title=title,
        description=excerpt if excerpt else notes,
        name=name,
        date_published=date_published,
        mantainer=mantainer,
        maintainer_email=maintainer_email,
        owner_division=owner_division,
        owner_email=owner_email,
        refresh_rate=refresh_rate,
        author=author,
        notes=notes,
        limitations=limitations,
        author_email=author_email,
        information_url=information_url,
        open_data_portal_url=open_data_portal_url,
        owner=owner,
        organization_entity_guid=organization_entity_guid,
        qualified_name=qualified_name,
        topics=_prepare_package_topics(
            topics=topics, keywords=keywords, allowed_tags=allowed_tags
        ),
        is_retired=is_retired,
        type_name=type_name,
        license_title=license_title if license_title else None,
        ckan_id=id,
        ckan_site_name=ckan_site_name,
    )

    return _get_or_create_entity(
        type_name=type_name,
        qualified_name=qualified_name,
        entity_name=name,
        create_if_new=True,
        entity_attributes=entity.attributes,
        relationship_attributes=entity.relationshipAttributes,
        classifications=entity.classifications,
        atlas_client=atlas_client,
        update_if_exists=update_if_exists,
    )


def _get_ckan_datastore_fields(
    resource_id: str,
    api_url: str,
    qualified_name: str,
):
    fields = []
    url = api_url + f"datastore_search?id={resource_id}&limit=0"
    res = requests.get(url)
    if res.status_code == 200:
        for f in res.json()["result"]["fields"]:
            if f["id"] == "_id":
                continue

            description = f"Belongs to {qualified_name}"
            if f.get("info"):
                notes = f["info"].get("notes")
                if notes:
                    description = _sanitized(notes)

            fields.append(
                AtlasStruct(
                    {
                        "type_name": "ckan_datastore_resource_schema",
                        "attributes": {
                            "name": f["id"],
                            "description": description,
                            "data_type": f["type"],
                        },
                    }
                )
            )

    return fields


def get_or_create_license_entity(
    ckan_site_name: str,
    name: str,
    ckan_id: str,
    description: str,
    owner: str,
    atlas_client,
    package_entity_guid: str,
    qualified_name: str,
    url: str = None,
    type_name: str = type_name_ckan_licenses,
    update_if_exists: bool = False,
):
    return _get_or_create_entity(
        type_name=type_name,
        qualified_name=qualified_name,
        entity_name=name,
        create_if_new=True,
        entity_attributes={
            "ckan_site_name": ckan_site_name,
            "name": name,
            "ckan_id": ckan_id,
            "description": description,
            "owner": owner,
            "qualifiedName": qualified_name,
            "url": url,
            "userDescription": description,
        },
        relationship_attributes={
            "package": AtlasRelatedObjectId({"guid": package_entity_guid}),
        },
        atlas_client=atlas_client,
        update_if_exists=update_if_exists,
        classifications=None,
    )


def _build_resource_entity(
    name: str,
    format: str,
    url: str,
    datastore_active: str,
    last_modified: str,
    resource_id: str,
    qualified_name: str,
    package_entity_guid: str,
    owner: str,
    cached_resources,
    description: str,
    filestore_resource_type_name: str,
    datastore_resource_type_name: str,
    ckan_site_name: str,
    classifications: list[str],
    api_url: str,
):
    datastore_active = str(datastore_active).lower().strip() == "true"

    entity = AtlasEntity(
        {
            "attributes": {
                "ckan_site_name": ckan_site_name,
                "name": name,
                "format": format,
                "description": description,
                "owner": owner,
                "url": url,
                "datastore_active": datastore_active,
                "last_modified": _convert_timestamp_to_epoch(last_modified),
                "ckan_id": resource_id,
                "qualifiedName": qualified_name,
                "userDescription": description,
            },
            "relationshipAttributes": {
                "package": AtlasRelatedObjectId({"guid": package_entity_guid}),
            },
        }
    )

    if datastore_active:
        entity.type_name = datastore_resource_type_name
        entity.attributes["cached_resources"] = cached_resources
        entity.attributes["data_dictionary"] = _get_ckan_datastore_fields(
            resource_id=resource_id, api_url=api_url, qualified_name=qualified_name
        )
    else:
        entity.type_name = filestore_resource_type_name

    if classifications:
        entity.classifications = [
            AtlasClassification(
                {
                    "typeName": c,
                    "propagate": False,
                    "removePropagationsOnEntityDelete": True,
                }
            )
            for c in classifications
        ]

    return entity


def get_or_create_ckan_resource_entity(
    name: str,
    format: str,
    url: str,
    datastore_active: str,
    last_modified: str,
    ckan_site_name: str,
    resource_id: str,
    qualified_name: str,
    package_entity_guid: str,
    owner: str,
    atlas_client,
    api_url: str,
    cached_resources=[],
    description: str = None,
    filestore_resource_type_name=type_name_ckan_resource,
    datastore_resource_type_name=type_name_ckan_datastore_resource,
    update_if_exists=False,
    classifications=None,
):
    entity = _build_resource_entity(
        name=name,
        format=format,
        url=url,
        datastore_active=datastore_active,
        last_modified=last_modified,
        resource_id=resource_id,
        qualified_name=qualified_name,
        package_entity_guid=package_entity_guid,
        owner=owner,
        description=description,
        filestore_resource_type_name=filestore_resource_type_name,
        datastore_resource_type_name=datastore_resource_type_name,
        cached_resources=cached_resources,
        classifications=classifications,
        ckan_site_name=ckan_site_name,
        api_url=api_url,
    )

    return _get_or_create_entity(
        type_name=datastore_resource_type_name
        if datastore_active
        else filestore_resource_type_name,
        qualified_name=qualified_name,
        entity_name=name,
        create_if_new=True,
        entity_attributes=entity.attributes,
        relationship_attributes=entity.relationshipAttributes,
        atlas_client=atlas_client,
        update_if_exists=update_if_exists,
        classifications=None,
    )


def run(
    api_url: str,
    ckan_site_url: str,
    portal_url: str,
    site_name: str,
    allowed_tags: list[str],
    package_limit: int = 1000,
    atlas_client=client,
    owner="chernandez",
    update_entities_if_exists=False,
    skip_packages=False,
):
    sanitized_site_name = _sanitized(site_name).lower().replace(" ", "-")
    ckan_site_qualified_name = f"{sanitized_site_name}@external"
    ckan_site = get_or_create_ckan_site_entity(
        name=site_name,
        ckan_site_name=site_name,
        api_url=api_url,
        site_url=ckan_site_url,
        qualified_name=ckan_site_qualified_name,
        portal_url=portal_url,
        type_name=type_name_ckan_site,
        atlas_client=atlas_client,
        owner=owner,
    )
    ckan_site_name = ckan_site.attributes["name"]

    orgs = {}
    for org in requests.get(api_url + "organization_list").json()["result"]:
        res = requests.get(api_url + f"organization_show?id={org}").json()["result"]
        org_qualified_name = f"{res['name']}.{sanitized_site_name}"

        orgs[res["name"]] = get_or_create_org_entity(
            title=res["display_name"],
            name=res["display_name"],
            approval_status=res["approval_status"],
            state=res["state"],
            ckan_site_entity_guid=ckan_site.guid,
            qualified_name=org_qualified_name,
            description=f"Organization belongs to {site_name}",
            owner=owner,
            atlas_client=atlas_client,
            update_if_exists=False,
            ckan_site_name=ckan_site_name,
        )

    if skip_packages:
        return ckan_site, list(orgs.values()), [], []

    pkg_entities = []
    packages = []
    catalogue = get_packages(api_url)
    if package_limit:
        catalogue = catalogue[:package_limit]

    for p in catalogue:
        org_qualified_name = orgs[p["organization"]["name"]]["attributes"][
            "qualifiedName"
        ]
        pkg_owner = p["owner_email"] if p.get("owner_email") else owner

        logging.debug(p["name"])
        # standardization between CKAN sites
        if p.get("date_published") is None:
            p["date_published"] = p["metadata_created"]
        if p.get("refresh_rate") is None:
            p["refresh_rate"] = p.get("update_frequency")
        if not p.get("owner_division"):
            p["owner_division"] = p["organization"]["name"]

        open_data_portal_url = None
        if "toronto" in org_qualified_name or "ontario" in org_qualified_name:
            open_data_portal_url = portal_url + "dataset/" + p["name"] + "/"
        elif "canada" in org_qualified_name:
            open_data_portal_url = portal_url + "data/en/dataset/" + p["name"] + "/"

        entity = get_or_create_pkg_entity(
            org_qualified_name=org_qualified_name,
            open_data_portal_url=open_data_portal_url,
            organization_entity_guid=orgs[p["organization"]["name"]].guid,
            owner=pkg_owner,
            atlas_client=atlas_client,
            update_if_exists=update_entities_if_exists,
            ckan_site_name=ckan_site_name,
            allowed_tags=allowed_tags,
            **p,
        )

        if p.get("license_id"):
            license_entity = get_or_create_license_entity(
                ckan_site_name=ckan_site_name,
                name=p.get("license_title"),
                ckan_id=p.get("license_id"),
                url=p.get("license_url"),
                description=None,
                owner=None,
                atlas_client=atlas_client,
                package_entity_guid=entity.guid,
                qualified_name=f"{p.get('license_id')}.{sanitized_site_name}",
                type_name=type_name_ckan_licenses,
                update_if_exists=update_entities_if_exists,
            )

        pkg_entities.append(entity)
        packages.append(p)

    logging.info(f"TOTAL PACKAGES: {len(packages)}")
    in_scope = {}
    for pkg, pkg_entity in zip(packages, pkg_entities):
        datastore_resources = [
            r
            for r in pkg["resources"]
            if str(r.get("datastore_active")).lower().strip() == "true"
        ]
        filestore_resources = [
            r
            for r in pkg["resources"]
            if str(r.get("datastore_active")).lower().strip() != "true"
        ]

        for r in datastore_resources + filestore_resources:
            if not r.get("name") or not r.get("url"):
                continue

            resc_qualified_name = (
                f'{r["name"]}.{pkg_entity.attributes["qualifiedName"]}'
            )
            datastore_active = str(r.get("datastore_active")).lower().strip() == "true"
            resource_format = (
                "datastore"
                if str(r.get("datastore_active")).lower().strip() == "true"
                else r["format"].lower()
            )
            last_modified = (
                r["last_modified"]
                if r.get("last_modified") is not None
                else r["created"]
            )
            description = f'Belongs to {pkg_entity.attributes["name"]}'
            ds_res_id = r.get("datastore_resource_id")

            if ds_res_id and in_scope.get(ds_res_id):
                cache_record = AtlasStruct(
                    {
                        "typeName": "ckan_datastore_cached",
                        "attributes": {
                            "ckan_id": r["id"],
                            "url": r["url"],
                            "format": r["format"].lower(),
                        },
                    }
                )

                if "cached_resources" not in in_scope[ds_res_id].attributes:
                    in_scope[ds_res_id].attributes["cached_resources"] = [cache_record]
                else:
                    in_scope[ds_res_id].attributes["cached_resources"].append(
                        cache_record
                    )

                atlas_client.entity.update_entity(
                    AtlasEntityWithExtInfo({"entity": in_scope[ds_res_id]})
                )
                continue

            in_scope[r["id"]] = get_or_create_ckan_resource_entity(
                name=r["name"],
                format="datastore",
                url=r["url"],
                datastore_active=datastore_active,
                last_modified=last_modified,
                resource_id=r["id"],
                qualified_name=resc_qualified_name,
                package_entity_guid=pkg_entity.guid,
                owner=pkg_entity.attributes["owner"],
                description=description,
                atlas_client=atlas_client,
                update_if_exists=update_entities_if_exists,
                ckan_site_name=ckan_site_name,
                api_url=api_url,
                classifications=None
                if not pkg_entity.classifications
                else [c["typeName"] for c in pkg_entity.classifications],
            )
    resource_entities = list(in_scope.values())

    logging.info(f"TOTAL RESOURCES: {len(resource_entities)}")

    return ckan_site, list(orgs.values()), pkg_entities, resource_entities


entity_results = {}

portals = [
    {
        "api_url": "https://open.toronto.ca/api/",
        "ckan_site_url": "https://ckan0.cf.opendata.inter.prod-toronto.ca/",
        "portal_url": "https://open.toronto.ca/",
        "site_name": "Open Data Toronto",
    },
    {
        "api_url": "https://data.ontario.ca/api/3/action/",
        "ckan_site_url": "https://data.ontario.ca/",
        "portal_url": "https://data.ontario.ca/",
        "site_name": "Government of Ontario Open Data",
    },
    {
        "api_url": "https://open.canada.ca/ckan/en/api/3/action/",
        "ckan_site_url": "https://search.open.canada.ca/opendata/",
        "portal_url": "https://open.canada.ca/",
        "site_name": "Government of Canada Open Data",
    },
]

for portal in portals:
    site, orgs, packages, resources = run(
        update_entities_if_exists=True,
        package_limit=None,
        skip_packages=False,
        allowed_tags=tags,
        **portal,
    )

    entity_results[portal["site_name"]] = {
        "site": site,
        "orgs": orgs,
        "packages": packages,
        "resources": resources,
    }
