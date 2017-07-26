# ocaml-influxdb

OCaml interface to the time series database InfluxDB.

**Not production ready!**. The library must be more tested.

## Examples.

All requests are done with the module `Influxdb.Client`.
```OCaml
(* Create a client for the database "foo" for an InfluxDB client running on port
   8087 (no HTTP request is done, it's only a record.
*)
let client = Influxdb.Client.create ~port:8087 ~database:"foo" () in
(* Create the database foo on the InfluxDB instance. *)
Influxdb.Client.create_database client "foo" >>=
(* Create a retention policy for 4 weeks *)
Influxdb.Client.create_retention_policy ~name:"1month" ~duration:"4w" client >>=
(* Create some points for the measurement cpu_load_short and write them *)
let measurement = Influxdb.Measurement.t_of_string "cpu_load_short" in
let fields = [Influxdb.Field.t_of_key_and_value "value" (Influxdb.Field.value_of_float 0.64)] in
let tags = [Influxdb.Tag.t_of_key_and_value "host" "server02";
            Influxdb.Tag.t_of_key_and_value "region" "us-west"] in
let time = Influxdb.Datetime.to_t ~year:2017 ~month:1 ~day:13 
    ~hour:12 ~minute:5 ~second:20 ~nanosecond:15678 in
let p = [Influxdb.Point.to_t measurement fields tags time] in
let time = Influxdb.Datetime.to_t ~year:2016 ~month:1 ~day:13
    ~hour:12 ~minute:5 ~second:20 ~nanosecond:15678 in
let p = Influxdb.Point.to_t measurement fields tags time :: p in
let time = Influxdb.Datetime.to_t ~year:2016 ~month:2 ~day:13
    ~hour:12 ~minute:5 ~second:20 ~nanosecond:15678 in
let p = Influxdb.Point.to_t measurement fields tags time :: p in
let time = Influxdb.Datetime.to_t ~year:2017 ~month:2 ~day:12 
    ~hour:12 ~minute:5 ~second:20 ~nanosecond:15678 in
let p = Influxdb.Point.to_t measurement fields tags time :: p in
let time = Influxdb.Datetime.to_t ~year:2016 ~month:2 ~day:12 
    ~hour:12 ~minute:52 ~second:20 ~nanosecond:15678 in
let p = Influxdb.Point.to_t measurement fields tags time :: p in
Influxdb.Client.write_points client p >>= fun _ ->
(* Get points with WHERE expression. Remove the parameter ~where for all points. *)
let from_time = Influxdb.Datetime.to_t ~year:2016 ~month:2 ~day:13 ~hour:12 ~minute:5 ~second:20 ~nanosecond:15678 in
let until_time = Influxdb.Datetime.to_t ~year:2017 ~month:2 ~day:13 ~hour:12 ~minute:5 ~second:20 ~nanosecond:15678 in
let open Influxdb.Where in
let where = [Time(GreaterOrEqual, from_time); Time(LessOrEqual, until_time)] in
let measurement = Influxdb.Measurement.t_of_string "cpu_load_short" in
Influxdb.Client.get_points client ~where measurement >>= fun points ->
Printf.printf "-- Number of points: %d\n" (List.length points);
List.iter print_endline (List.map Influxdb.Point.line_of_t points); Lwt.return ()
```


