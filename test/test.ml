open Lwt.Infix

let create_database client =
  Influxdb.Client.Raw.create_database client (Influxdb.Client.database_of_t client)

let create_and_remove_database client =
  let database = Influxdb.Client.database_of_t client in
  Influxdb.Client.Raw.create_database client database >>= fun resp ->
  Influxdb.Client.get_all_database_names client >>= fun names ->
  if List.mem database names
  then (
    Influxdb.Client.drop_database client database >>= fun () ->
    Influxdb.Client.get_all_database_names client >>= fun names ->
    if List.mem database names
    then (Printf.printf "Error: %s not dropped\n" database; Lwt.return true)
    else (Printf.printf "OK\n"; Lwt.return true)
  )
  else (Printf.printf "Error: %s not created\n" database; Lwt.return false)

let show_points client =
  let measurement = Influxdb.Measurement.t_of_string "cpu_load_short" in
  Influxdb.Client.get_points client measurement >>= fun points ->
  Printf.printf "-- Number of points: %d\n" (List.length points);
  List.iter print_endline (List.map Influxdb.Point.line_of_t points); Lwt.return ()

let drop_measurement client =
  (* Not working. Need to escape characters. *)
  (* Influxdb.Client.drop_measurement client (Influxdb.Measurement.t_of_string "q=cpu_load_short") *)
  Influxdb.Client.drop_measurement client (Influxdb.Measurement.t_of_string "cpu_load_short")

let show_measurement client =
  Influxdb.Client.get_all_measurements client >>= fun measurements ->
  List.iter print_endline (List.map Influxdb.Measurement.string_of_t measurements);
  Lwt.return ()

let write_some_points client =
  let measurement = Influxdb.Measurement.t_of_string "cpu_load_short" in
  let fields = [Influxdb.Field.t_of_key_and_value "value" (Influxdb.Field.value_of_float 0.64)] in
  let tags = [Influxdb.Tag.t_of_key_and_value "host" "server02"; Influxdb.Tag.t_of_key_and_value "region" "us-west"] in
  let time = Influxdb.Datetime.to_t ~year:2017 ~month:1 ~day:13 ~hour:12 ~minute:5 ~second:20 ~nanosecond:15678 in
  let p = [Influxdb.Point.to_t measurement fields tags time] in
  let time = Influxdb.Datetime.to_t ~year:2016 ~month:1 ~day:13 ~hour:12 ~minute:5 ~second:20 ~nanosecond:15678 in
  let p = Influxdb.Point.to_t measurement fields tags time :: p in
  let time = Influxdb.Datetime.to_t ~year:2016 ~month:2 ~day:13 ~hour:12 ~minute:5 ~second:20 ~nanosecond:15678 in
  let p = Influxdb.Point.to_t measurement fields tags time :: p in
  let time = Influxdb.Datetime.to_t ~year:2017 ~month:2 ~day:12 ~hour:12 ~minute:5 ~second:20 ~nanosecond:15678 in
  let p = Influxdb.Point.to_t measurement fields tags time :: p in
  let time = Influxdb.Datetime.to_t ~year:2016 ~month:2 ~day:12 ~hour:12 ~minute:52 ~second:20 ~nanosecond:15678 in
  let p = Influxdb.Point.to_t measurement fields tags time :: p in
  Influxdb.Client.write_points client p >>= fun _ -> Lwt.return_unit

let get_points_where client =
  let from_time = Influxdb.Datetime.to_t ~year:2016 ~month:2 ~day:13 ~hour:12 ~minute:5 ~second:20 ~nanosecond:15678 in
  let until_time = Influxdb.Datetime.to_t ~year:2017 ~month:2 ~day:13 ~hour:12 ~minute:5 ~second:20 ~nanosecond:15678 in
  let open Influxdb.Where in
  let where = [Time(GreaterOrEqual, from_time); Time(LessOrEqual, until_time)] in
  let measurement = Influxdb.Measurement.t_of_string "cpu_load_short" in
  Influxdb.Client.get_points client ~where measurement >>= fun points ->
  Printf.printf "-- Number of points: %d\n" (List.length points);
  List.iter print_endline (List.map Influxdb.Point.line_of_t points); Lwt.return ()

let _ =
  Lwt_main.run (
    let client = Influxdb.Client.create ~port:8087 ~database:"foofoo" () in
    create_and_remove_database client >>= fun _ ->
    create_database client >>= fun _ ->
    drop_measurement client >>= fun _ ->
    show_measurement client >>= fun _ ->
    write_some_points client >>= fun _ ->
    show_measurement client >>= fun _ ->
    show_points client >>= fun _ ->
    get_points_where client >>= fun _ ->
    Lwt.return ()
  )
