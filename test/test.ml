open Lwt.Infix

let create_and_remove_database client =
  let database = "test42" in
  Influxdb.Client.Raw.create_database client database >>= fun resp ->
  print_endline resp;
  Influxdb.Client.get_all_database_names client >>= fun names ->
  List.iter print_endline names;
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
    let client = Influxdb.Client.create ~port:8087 ~database:"test3" () in
    ignore (create_and_remove_database client);
    (*
    Influxdb.Client.create_database client "test4" >>= fun resp ->
    Influxdb.Client.Raw.create_retention_policy ~name:"week" ~duration:"4w" client >>= fun resp ->
    print_endline resp;
    Influxdb.Client.get_all_retention_policies client >>= fun resp ->
    List.iter
      (fun retention_policy ->
         print_endline (Influxdb.RetentionPolicy.name_of_t retention_policy);
         print_endline (Influxdb.RetentionPolicy.duration_of_t retention_policy);
         print_endline (string_of_int @@ Influxdb.RetentionPolicy.replicant_of_t retention_policy);
         print_endline (string_of_bool @@ Influxdb.RetentionPolicy.is_default retention_policy);
      )
      resp;
    (* Influxdb.Client.get_all_database_names client >>= fun(database_list) -> *)
    (* List.iter print_endline database_list; *)
    *)
    Lwt.return ()
  )
