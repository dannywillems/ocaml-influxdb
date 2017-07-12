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
    then (Printf.printf "Error: %s not dropped" database; Lwt.return true)
    else (Printf.printf "OK"; Lwt.return true)
  )
  else (Printf.printf "Error: %s not created" database; Lwt.return false)


let _ =
  Lwt_main.run (
    let client = Influxdb.Client.create ~port:8087 ~database:"test" () in
    create_and_remove_database client >>= fun b ->
    Influxdb.Client.Raw.get_all_measurements client >>= fun str ->
    print_endline str;
    Lwt.return ()
  )
