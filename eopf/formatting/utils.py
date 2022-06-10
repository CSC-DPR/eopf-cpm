from typing import List

from shapely.affinity import translate
from shapely.geometry import MultiPolygon, Polygon


def poly_coords_parsing(a_string: str) -> List[List[float]]:
    """Used to parse a string with coordinates and convert it to a list of points (latitude, longitude)

    Parameters
    ----------
    a_string: str
        String (xpath ouput usually) with posList coordinates (separated by a white space)

    Returns
    ----------
    List[List[float]]
        List containg pairs of coordinates casted to floating point representation (latitude, longitude)
    """
    word_index = 0
    coords_list = []
    string_len = len(a_string)
    cur_lat = ""
    cur_lon = ""

    # remove begining spaces
    start = 0
    while start < string_len - 1:
        if a_string[start] == " ":
            start += 1
        else:
            break

    # remove ending spaces
    stop = string_len
    while stop >= 0:
        if a_string[stop - 1] == " ":
            stop -= 1
        else:
            break

    for i in range(start, stop, 1):
        if a_string[i] != " ":
            # when you have spaces or digits, also other charecters are accepted but the conversion will fail
            if word_index == 0:
                cur_lat += a_string[i]
            else:
                cur_lon += a_string[i]
        else:
            # when you have spaces

            # just in case you have multiple spaces
            if a_string[i + 1] == " ":
                continue

            if word_index == 0:
                word_index += 1
            else:
                coords_list.append([float(cur_lat), float(cur_lon)])
                word_index = 0
                cur_lat = ""
                cur_lon = ""

    # the last coordinates are appended here
    coords_list.append([float(cur_lat), float(cur_lon)])
    return coords_list


def detect_pole_or_antemeridian(coordinates: List[List[float]]) -> bool:
    """Verify if a list of coordinates crosses a antemeridian or a pole

    Parameters
    ----------
    coordinates: List[List[float]]
        List containg pairs of coordinates (latitude, longitude)

    Returns
    ----------
    bool:
        True if coordinates croos pole/antemeridian at least once, False otherwise
    """
    import itertools

    longitude_threshold = 270
    crossing = 0
    # Flat coordinates in order to iterate only over longitudes
    flatten_coords = [longitude for longitude in itertools.chain.from_iterable(coordinates)]
    # Compare absolute difference of longitude[i+1], longitude[i] with threshold
    for current_longitude, next_longitude in zip(flatten_coords[1::2], flatten_coords[3::2]):
        longitude_difference = abs(next_longitude - current_longitude)
        if longitude_difference > longitude_threshold:
            crossing += 1

    return crossing >= 1


def split_poly(polygon: Polygon) -> MultiPolygon:
    the_planet = Polygon([[-180, 90], [180, 90], [180, -90], [-180, -90], [-180, 90]])
    shifted_planet = Polygon([[180, 90], [540, 90], [540, -90], [180, -90], [180, 90]])
    normalized_points = []
    for point in polygon.exterior.coords:
        lon = point[0]
        if lon < 0.0:
            lon += 360.0
        normalized_points.append([lon, point[1]])

    normalized_polygon = Polygon(normalized_points)

    # cut out eastern part (up to 180 deg)
    intersection_east = the_planet.intersection(normalized_polygon)

    # cut out western part - shifted by 360 deg using the shifted planet boundary
    # and shift the intersection back westwards to the -180-> 180 deg range
    intersection_west = shifted_planet.intersection(normalized_polygon)
    shifted_back = translate(intersection_west, -360.0, 0, 0)

    return MultiPolygon([intersection_east, shifted_back])
