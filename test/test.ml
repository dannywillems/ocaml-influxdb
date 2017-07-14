open Lwt.Infix

let create_and_remove_database client =
  let database = "test42" in
  Influxdb.Client.Raw.create_database client database >>= fun resp ->
  Influxdb.Client.get_all_database_names client >>= fun names ->
  if List.mem database names
  then (
    Influxdb.Client.drop_database client database >>= fun () ->
    Influxdb.Client.get_all_database_names client >>= fun names ->
    if List.mem database names
    then (Printf.printf "Error: %s not dropped\n" database; Lwt.return true)
    else (Printf.printf "OK"; Lwt.return true)
  )
  else (Printf.printf "Error: %s not created\n" database; Lwt.return false)

let _ =
  Lwt_main.run (
    let client = Influxdb.Client.create ~port:8087 ~database:"test" () in
    create_and_remove_database client >>= fun b ->
    Influxdb.Client.get_all_measurements client >>= fun measurements ->
    let measurement = List.hd measurements in
    let measurements = List.map Influxdb.Measurement.string_of_t measurements in
    List.iter print_endline measurements;
    Influxdb.Client.get_tag_names_of_measurement client measurement >>= fun resp ->
    List.iter print_endline resp;
    Influxdb.Client.get_field_names_of_measurement client measurement >>= fun resp ->
    List.iter print_endline resp;
    Influxdb.Client.Raw.get_points client measurement >>= fun resp ->
    print_endline resp;
    Lwt.return ()
  )
