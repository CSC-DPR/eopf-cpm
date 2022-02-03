from typing import Any

# flake8: noqa

CF_MAP_OLCI = {
    "title": "concat('',metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/sentinel3:generalProductInformation/sentinel3:productName/text())",
    "history": "concat(metadataSection/metadataObject[@ID='processing']/metadataWrap/xmlData/sentinel-safe:processing/sentinel-safe:facility/sentinel-safe:software/@name,"
    + "' ',metadataSection/metadataObject[@ID='processing']/metadataWrap/xmlData/sentinel-safe:processing/sentinel-safe:facility/sentinel-safe:software/@version,"
    + "' ',metadataSection/metadataObject[@ID='processing']/metadataWrap/xmlData/sentinel-safe:processing/@stop)",
    "institution": "concat(metadataSection/metadataObject[@ID='processing']/metadataWrap/xmlData/sentinel-safe:processing/sentinel-safe:facility/@organisation,"
    + "', ',metadataSection/metadataObject[@ID='processing']/metadataWrap/xmlData/sentinel-safe:processing/sentinel-safe:facility/@name)",
    "source": "concat(metadataSection/metadataObject[@ID='platform']/metadataWrap/xmlData/sentinel-safe:platform/sentinel-safe:familyName/text(), "
    + "metadataSection/metadataObject[@ID='platform']/metadataWrap/xmlData/sentinel-safe:platform/sentinel-safe:number/text(), "
    + "' ',metadataSection/metadataObject[@ID='platform']/metadataWrap/xmlData/sentinel-safe:platform/sentinel-safe:instrument/sentinel-safe:familyName/@abbreviation,"
    + "' ',metadataSection/metadataObject[@ID='platform']/metadataWrap/xmlData/sentinel-safe:platform/sentinel-safe:instrument/sentinel-safe:familyName/text())",
    "comment": "concat('',metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/sentinel3:generalProductInformation/sentinel3:dispositionMode/text())",
    "references": "'https://sentinels.copernicus.eu/web/sentinel/missions/sentinel-3,"
    + " https://sentinels.copernicus.eu/web/sentinel/user-guides/sentinel-3-olci/processing-levels/level-1'",
    "Conventions": "'CF-1.9'",
}
EOP_MAP_OLCI: dict[Any, Any] = {
    "phenomenonTime": {
        "beginPosition": "metadataSection/metadataObject[@ID='acquisitionPeriod']//sentinel-safe:acquisitionPeriod/sentinel-safe:startTime",
        "endPosition": "metadataSection/metadataObject[@ID='acquisitionPeriod']//sentinel-safe:acquisitionPeriod/sentinel-safe:stopTime",
    },
    "resultTime": {
        "timePosition": "metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/sentinel3:generalProductInformation/sentinel3:creationTime",
    },
    "procedure": {
        "platform": {
            "shortName": "metadataSection/metadataObject[@ID='platform']/metadataWrap/xmlData/sentinel-safe:platform/sentinel-safe:familyName",
            "serialIdentifier": "metadataSection/metadataObject[@ID='platform']/metadataWrap/xmlData/sentinel-safe:platform/sentinel-safe:number",
        },
        "instrument": {
            "shortName": "metadataSection/metadataObject[@ID='platform']/metadataWrap/xmlData/sentinel-safe:platform/sentinel-safe:instrument/sentinel-safe:familyName/@abbreviation",
        },
        "sensor": {
            "sensorType": "'OPTICAL'",
            "operationalMode": "metadataSection/metadataObject[@ID='platform']/metadataWrap/xmlData/sentinel-safe:platform/sentinel-safe:instrument/sentinel-safe:mode/@identifier",
        },
        "acquistionParameters": {
            "orbitNumber": "metadataSection/metadataObject[@ID='measurementOrbitReference']/metadataWrap/xmlData/sentinel-safe:orbitReference/sentinel-safe:orbitNumber",
            "orbitDirection": "metadataSection/metadataObject[@ID='measurementOrbitReference']/metadataWrap/xmlData/sentinel-safe:orbitReference/sentinel-safe:orbitNumber/@groundTrackDirection",
        },
    },
    "featureOfInterest": {
        "multiExtentOf": "metadataSection/metadataObject[@ID='measurementFrameSet']/metadataWrap/xmlData/sentinel-safe:frameSet/sentinel-safe:footPrint/gml:posList",
    },
    "result": {
        "product": {
            "fileName": "dataObjectSection/dataObject/byteStream/fileLocation/@href",
            "timeliness": "metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/sentinel3:generalProductInformation/sentinel3:timeliness",
        },
    },
    "metadataProperty": {
        "identifier": "metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/sentinel3:generalProductInformation/sentinel3:productName",
        "creationDate": "metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/sentinel3:generalProductInformation/sentinel3:creationTime",
        "acquisitionType": "metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/sentinel3:generalProductInformation/sentinel3:dispositionMode",
        "productType": "metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/sentinel3:generalProductInformation/sentinel3:productType",
        "status": "'ARCHIVED'",
        "downlinkedTo": {
            "acquisitionStation": "metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/sentinel3:generalProductInformation/sentinel3:dumpInformation/sentinel3:receivingGroundStation",
            "acquisitionDate": "metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/sentinel3:generalProductInformation/sentinel3:dumpInformation/sentinel3:receivingStopTime",
        },
        "productQualityStatus": "metadataSection/metadataObject[@ID='measurementQualityInformation']/metadataWrap/xmlData/sentinel-safe:qualityInformation/sentinel-safe:extension/sentinel3:productQuality/sentinel3:onlineQualityCheck",
        "productQualityDegradationTag": "metadataSection/metadataObject[@ID='measurementQualityInformation']/metadataWrap/xmlData/sentinel-safe:qualityInformation/sentinel-safe:extension/sentinel3:productQuality/sentinel3:degradationFlags",
        "processing": {
            "processingCenter": "metadataSection/metadataObject[@ID='processing']/metadataWrap/xmlData/sentinel-safe:processing/sentinel-safe:facility/@name",
            "processingDate": "metadataSection/metadataObject[@ID='processing']/metadataWrap/xmlData/sentinel-safe:processing/@stop",
            "processorName": "metadataSection/metadataObject[@ID='processing']/metadataWrap/xmlData/sentinel-safe:processing/sentinel-safe:facility/sentinel-safe:software/@name",
            "processorVersion": "metadataSection/metadataObject[@ID='processing']/metadataWrap/xmlData/sentinel-safe:processing/sentinel-safe:facility/sentinel-safe:software/@version",
        },
    },
}
NAMESPACES_OLCI = {
    "xfdu": "urn:ccsds:schema:xfdu:1",
    "gml": "http://www.opengis.net/gml",
    "sentinel-safe": "http://www.esa.int/safe/sentinel/1.1",
    "sentinel3": "http://www.esa.int/safe/sentinel/sentinel-3/1.0",
    "olci": "http://www.esa.int/safe/sentinel/sentinel-3/olci/1.0",
}

CF_MAP_SLSTR_L1 = {
    "title": "concat('',metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/sentinel3:generalProductInformation/sentinel3:productName/text())",
    "history": "concat(metadataSection/metadataObject[@ID='processing']/metadataWrap/xmlData/sentinel-safe:processing/sentinel-safe:facility/sentinel-safe:software/@name,"
    + "' ',metadataSection/metadataObject[@ID='processing']/metadataWrap/xmlData/sentinel-safe:processing/sentinel-safe:facility/sentinel-safe:software/@version,"
    + "' ',metadataSection/metadataObject[@ID='processing']/metadataWrap/xmlData/sentinel-safe:processing/@stop)",
    "institution": "concat(metadataSection/metadataObject[@ID='processing']/metadataWrap/xmlData/sentinel-safe:processing/sentinel-safe:facility/@organisation,"
    + "', ',metadataSection/metadataObject[@ID='processing']/metadataWrap/xmlData/sentinel-safe:processing/sentinel-safe:facility/@name)",
    "source": "concat(metadataSection/metadataObject[@ID='platform']/metadataWrap/xmlData/sentinel-safe:platform/sentinel-safe:familyName/text(), "
    + "metadataSection/metadataObject[@ID='platform']/metadataWrap/xmlData/sentinel-safe:platform/sentinel-safe:number/text(), "
    + "' ',metadataSection/metadataObject[@ID='platform']/metadataWrap/xmlData/sentinel-safe:platform/sentinel-safe:instrument/sentinel-safe:familyName/@abbreviation,"
    + "' ',metadataSection/metadataObject[@ID='platform']/metadataWrap/xmlData/sentinel-safe:platform/sentinel-safe:instrument/sentinel-safe:familyName/text())",
    "comment": "concat('',metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/sentinel3:generalProductInformation/sentinel3:dispositionMode/text())",
    "references": "'https://sentinels.copernicus.eu/web/sentinel/missions/sentinel-3,"
    + " https://sentinels.copernicus.eu/web/sentinel/user-guides/Sentinel-3-slstr/processing-levels/level-1'",
    "Conventions": "'CF-1.9'",
}

EOP_MAP_SLSTR_L1: dict[Any, Any]  = {
    "phenomenonTime": {
        "beginPosition": "metadataSection/metadataObject[@ID='acquisitionPeriod']//sentinel-safe:acquisitionPeriod/sentinel-safe:startTime",
        "endPosition": "metadataSection/metadataObject[@ID='acquisitionPeriod']//sentinel-safe:acquisitionPeriod/sentinel-safe:stopTime",
    },
    "resultTime": {
        "timePosition": "metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/sentinel3:generalProductInformation/sentinel3:creationTime",
    },
    "procedure": {
        "platform": {
            "shortName": "metadataSection/metadataObject[@ID='platform']/metadataWrap/xmlData/sentinel-safe:platform/sentinel-safe:familyName",
            "serialIdentifier": "metadataSection/metadataObject[@ID='platform']/metadataWrap/xmlData/sentinel-safe:platform/sentinel-safe:number",
        },
        "instrument": {
            "shortName": "metadataSection/metadataObject[@ID='platform']/metadataWrap/xmlData/sentinel-safe:platform/sentinel-safe:instrument/sentinel-safe:familyName/@abbreviation",
        },
        "sensor": {
            "sensorType": "'OPTICAL'",
            "operationalMode": "metadataSection/metadataObject[@ID='platform']/metadataWrap/xmlData/sentinel-safe:platform/sentinel-safe:instrument/sentinel-safe:mode/@identifier",
        },
        "acquistionParameters": {
            "orbitNumber": "metadataSection/metadataObject[@ID='measurementOrbitReference']/metadataWrap/xmlData/sentinel-safe:orbitReference/sentinel-safe:orbitNumber",
            "orbitDirection": "metadataSection/metadataObject[@ID='measurementOrbitReference']/metadataWrap/xmlData/sentinel-safe:orbitReference/sentinel-safe:orbitNumber/@groundTrackDirection",
        },
    },
    "featureOfInterest": {
        "multiExtentOf": "metadataSection/metadataObject[@ID='measurementFrameSet']/metadataWrap/xmlData/sentinel-safe:frameSet/sentinel-safe:footPrint/gml:posList",
    },
    "result": {
        "product": {
            "fileName": "dataObjectSection/dataObject/byteStream/fileLocation/@href",
            "timeliness": "metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/sentinel3:generalProductInformation/sentinel3:timeliness",
        },
    },
    "metadataProperty": {
        "identifier": "metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/sentinel3:generalProductInformation/sentinel3:productName",
        "creationDate": "metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/sentinel3:generalProductInformation/sentinel3:creationTime",
        "acquisitionType": "metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/sentinel3:generalProductInformation/sentinel3:dispositionMode",
        "productType": "metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/sentinel3:generalProductInformation/sentinel3:productType",
        "status": "'ARCHIVED'",
        "downlinkedTo": {
            "acquisitionStation": "metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/sentinel3:generalProductInformation/sentinel3:dumpInformation/sentinel3:receivingGroundStation",
            "acquisitionDate": "metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/sentinel3:generalProductInformation/sentinel3:dumpInformation/sentinel3:receivingStopTime",
        },
        "productQualityStatus": "metadataSection/metadataObject[@ID='measurementQualityInformation']/metadataWrap/xmlData/sentinel-safe:qualityInformation/sentinel-safe:extension/sentinel3:productQuality/sentinel3:onlineQualityCheck",
        "productQualityDegradationTag": "metadataSection/metadataObject[@ID='measurementQualityInformation']/metadataWrap/xmlData/sentinel-safe:qualityInformation/sentinel-safe:extension/sentinel3:productQuality/sentinel3:degradationFlags",
        "processing": {
            "processingCenter": "metadataSection/metadataObject[@ID='processing']/metadataWrap/xmlData/sentinel-safe:processing/sentinel-safe:facility/@name",
            "processingDate": "metadataSection/metadataObject[@ID='processing']/metadataWrap/xmlData/sentinel-safe:processing/@stop",
            "processorName": "metadataSection/metadataObject[@ID='processing']/metadataWrap/xmlData/sentinel-safe:processing/sentinel-safe:facility/sentinel-safe:software/@name",
            "processorVersion": "metadataSection/metadataObject[@ID='processing']/metadataWrap/xmlData/sentinel-safe:processing/sentinel-safe:facility/sentinel-safe:software/@version",
        },
    },
}

NAMESPACES_SLSTR_L1 = {
    "xfdu": "urn:ccsds:schema:xfdu:1",
    "gml": "http://www.opengis.net/gml",
    "sentinel-safe": "http://www.esa.int/safe/sentinel/1.1",
    "sentinel3": "http://www.esa.int/safe/sentinel/sentinel-3/1.0",
    "slstr": "http://www.esa.int/safe/sentinel/sentinel-3/slstr/1.0",
}
