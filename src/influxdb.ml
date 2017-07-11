open Lwt.Infix

module Json = Yojson.Basic

module Precision = struct
  type t =
    | Second
    | Millisecond
    | Microsecond
    | Nanosecond

  let string_of_t t = match t with
    | Second -> "s"
    | Millisecond -> "ms"
    | Microsecond -> "us"
    | Nanosecond -> "ns"

  let t_of_string s = match s with
    | "s" -> Second
    | "ms" -> Millisecond
    | "us" -> Microsecond
    | "ns" -> Nanosecond
    | _ -> failwith (Printf.sprintf "Precision %s is not supported. Only s, ms, us and ns are supported." s)
end

module Field = struct
  type key = string

  type value =
    | Float of float
    | String of string
    | Bool of bool
    | Int of int

  type t = key * value

  let value_of_string s = String s
  let value_of_bool b = Bool b
  let value_of_float f = Float f
  let value_of_int i = Int i

  let key_of_field (k, _) = k
  let value_of_field (_, v) = v

  let to_string (key, value) =
    Printf.sprintf
      "%s=%s"
      key
      value
end

module Tag = struct
  (** The key of a tag. *)
  type key = string
  (** The value of a tag. *)
  type value = string

  (** A field is a couple (key, value) *)
  type t = key * value

  let of_key_and_value k v = (k, v)

  let to_string (key, value) =
    Printf.sprintf
      "%s=%s"
      key
      value
end

module RetentionPolicy = struct
  type t = {
    name: string;
    duration: string;
    shard_group_duration: string;
    replicant: int;
    default: bool;
  }

  let name_of_t t = t.name
  let duration_of_t t = t.duration
  let replicant_of_t t = t.replicant
  let is_default t = t.default
  let shard_group_duration_of_t t = t.shard_group_duration

  let t_of_tuple (name, duration, shard_group_duration, replicant, default) = {
    name;
    duration;
    shard_group_duration;
    replicant;
    default;
  }


  let t_of_json json =
    match (Json.Util.to_list json) with
    | name :: duration :: shard_group_duration :: replicant :: default :: [] ->
      t_of_tuple (
        (Json.Util.to_string name),
        (Json.Util.to_string duration),
        (Json.Util.to_string shard_group_duration),
        (Json.Util.to_int replicant),
        (Json.Util.to_bool default)
      )
    | _ -> failwith "Failed when fetching retention policies. Did the format change?"

  let to_t ~name ~duration ~shard_group_duration ~replicant ~default = {
    name;
    duration;
    replicant;
    default;
    shard_group_duration
  }

end

module Measurement = struct
  (** The type of a measurement. *)
  type t = {
    name: string;
    retention_policy: RetentionPolicy.t
  }

  let to_t name retention_policy = {
    name; retention_policy
  }

  let retention_policy_of_t m = m.retention_policy

  let name_of_t m = m.name
end

module Point = struct
  (** The type of point. *)
  type t = {
    measurement: Measurement.t;
    fields: Field.t list;
    tags: Tag.t list;
    timestamp: int64
  }

  (** Get the measurement of a point. *)
  let measurement_of_point point = point.measurement

  (** Get the fields of a point. *)
  let fields_of_point point = point.fields

  (** Get the tags of a point. *)
  let tags_of_point point = point.tags

  (** Get the timestamp of the point. *)
  let timestamp_of_point point = point.timestamp

  let line_of_point point =
    "TODO"
end

module Series = struct
  type t = {
    name: string;
    columns: string list;
    values: Point.t list
  }

  let of_json json =
    let columns =
      List.map
        Json.Util.to_string
        (Json.Util.member "columns" json |> Json.Util.to_list)
    in
    ()
end

module Client = struct
  type t = {
    username: string;
    password: string;
    host: string;
    port: int;
    database: string
  }

  let url client use_https =
    let protocol = if use_https then "https" else "http" in
    Printf.sprintf
      "%s://%s:%d"
      protocol
      client.host
      client.port

  let get_request ?(use_https=false) t request =
    let base_url = url t use_https in
    let url =
      Printf.sprintf
        "%s/query?db=%s&q=%s"
        base_url
        t.database
        request
    in
    Cohttp_lwt_unix.Client.get (Uri.of_string url) >>= fun(response, body) ->
      let code = response |> Cohttp.Response.status |> Cohttp.Code.code_of_status in
      let body = Cohttp_lwt_body.to_string body in
      body

  let raw_series_of_raw_result str =
    let json = Json.from_string str in
    let results = Json.Util.member "results" json |> Json.Util.to_list in
    (Json.Util.member "series" (List.hd results)) |> Json.Util.to_list

  let json_of_result str =
    Json.from_string str

  let post_request ?(use_https=false) client data =
    let body_str =
      Printf.sprintf
        "q=%s"
        data
    in
    let body = Cohttp_lwt_body.of_string body_str in
    let base_url = url client use_https in
    let url = Printf.sprintf
        "%s/query"
        base_url
    in
    (* The headers are mandatory! Else, we will receive the error
       {"error":"missing required parameter \"q\""}
    *)
    let headers = Cohttp.Header.init () in
    let headers = Cohttp.Header.add headers "Content-Type" "application/x-www-form-urlencoded" in
    Cohttp_lwt_unix.Client.post ~body ~headers (Uri.of_string url) >>= fun (response, body) ->
    Cohttp_lwt_body.to_string body

  let create ?(username="root") ?(password="root") ?(host="localhost") ?(port=8086) ~database () = {
      username; password; host; port; database
    }

  let switch_database client database = {
    username = client.username;
    password = client.password;
    host = client.host;
    port = client.port;
    database = database
  }

  (* let get_all_retention_policies client = *)
    (* () *)

  (* let create_retention_policies client ~name ~database = *)
  (*   () *)

  (* let write_points client points = *)
  (*   () *)

  module Raw = struct
    let create_database client database_name =
      let str = Printf.sprintf
          "CREATE DATABASE %s"
          database_name
      in
      post_request client str

    let get_all_points_of_measurement client measurement =
      let str = Printf.sprintf
          "SELECT * FROM %s.%s.%s"
          client.database
          (RetentionPolicy.name_of_t (Measurement.retention_policy_of_t measurement))
          (Measurement.name_of_t measurement)
      in
      get_request client str

    let get_all_database_names client =
      get_request client "SHOW DATABASES"

    let get_default_retention_policy_on_database client =
      ()

    let get_all_retention_policies_of_database client =
      let str = Printf.sprintf
          "SHOW RETENTION POLICIES ON %s"
          client.database
      in
      get_request client str

    let create_retention_policy ?(default = false) ?(replicant=1) ~name ~duration client =
      let str_default = if default then "DEFAULT" else "" in
      let str = Printf.sprintf
          "CREATE RETENTION POLICY %s \
           ON %s \
           DURATION %s \
           REPLICATION %d \
           %s"
          name
          client.database
          duration
          replicant
          str_default
      in
      post_request client str

    let drop_retention_policy client name =
      let str = Printf.sprintf
          "DROP RETENTION POLICY %s ON %s"
          name
          client.database
      in
      get_request client str

    let drop_database client name =
      let str = Printf.sprintf
          "DROP DATABASE %s"
          name
      in
      post_request client str
  end

  let create_database client database_name =
    Raw.create_database client database_name >>= fun body_str ->
    let json = json_of_result body_str in
    Lwt.return ()

  let get_all_database_names client =
    (Raw.get_all_database_names client) >>= fun body_str ->
    let series = raw_series_of_raw_result body_str |> List.hd in
    let values =
      List.map
        (fun value_as_list ->
           Json.Util.to_list value_as_list |> List.hd |> Json.Util.to_string
        )
        (Json.Util.member "values" series |> Json.Util.to_list)
    in
    Lwt.return values

  let get_all_retention_policies client =
    Raw.get_all_retention_policies_of_database client >>= fun str ->
    let series = raw_series_of_raw_result str |> List.hd
    in
    (* Can be useful later to check the columns are the same than we need *)
    (* let columns = *)
    (*   Json.Util.member "columns" series *)
    (*   |> Json.Util.to_list *)
    (* in *)
    let list_of_values json =
      List.map RetentionPolicy.t_of_json json
    in
    let values = list_of_values (Json.Util.member "values" series |> Json.Util.to_list) in
    Lwt.return values

  let create_retention_policy ?(default = false) ?(replicant=1) ~name ~duration client =
    Raw.create_retention_policy ~default ~replicant ~name ~duration client >>= fun resp -> Lwt.return ()

  let drop_retention_policy client name =
    Raw.drop_retention_policy client name >>= fun resp -> Lwt.return ()

  let drop_database client name =
    Raw.drop_database client name >>= fun resp -> Lwt.return ()

  (* Add a timestamp. Or maybe, use something more complicated. *)
  (* let get_all_tags_of_measurement client measurement = *)
  (*   () *)

  (* let get_all_fields_of_measurement client measurement = *)
  (*   () *)
end
