CREATE TABLE IF NOT EXISTS lst(
    rid serial primary key,
    rast raster,
    timeslot timestamp
);
CREATE INDEX lst_rast_st_convexhull_idx ON lst USING gist( ST_ConvexHull(rast) );

CREATE TABLE IF NOT EXISTS lst_frac_proc_pixels(
    rid serial primary key,
    rast raster,
    timeslot timestamp
);
CREATE INDEX lst_frac_proc_pixels_rast_st_convexhull_idx ON lst_frac_proc_pixels USING gist( ST_ConvexHull(rast) );

CREATE TABLE IF NOT EXISTS lst_q_flags(
    rid serial primary key,
    rast raster,
    timeslot timestamp
);
CREATE INDEX lst_q_flags_rast_st_convexhull_idx ON lst_q_flags USING gist( ST_ConvexHull(rast) );

CREATE TABLE IF NOT EXISTS lst_errorbar_lst(
    rid serial primary key,
    rast raster,
    timeslot timestamp
);
CREATE INDEX lst_errorbar_lst_rast_st_convexhull_idx ON lst_errorbar_lst USING gist( ST_ConvexHull(rast) );

CREATE TABLE IF NOT EXISTS lst_time(
    rid serial primary key,
    rast raster,
    timeslot timestamp
);
CREATE INDEX lst_time_rast_st_convexhull_idx ON lst_time USING gist( ST_ConvexHull(rast) );
