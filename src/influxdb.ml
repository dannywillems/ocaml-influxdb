open Lwt.Infix

(** Hash table used to split points based on the retention policies. *)
module MapString = Map.Make(String)

module Json = Yojson.Basic

module Datetime = struct
  type t = {
    year: int;
    month: int;
    day: int;
    hour: int;
    minute: int;
    second: int
  }

  let string_of_t t =
    Printf.sprintf
      "%s-%s-%s %s:%s:%s"
      (string_of_int t.year)
      (string_of_int t.month)
      (string_of_int t.day)
      (string_of_int t.hour)
      (string_of_int t.minute)
      (string_of_int t.second)

  let to_t ~year ~month ~day ~hour ~minute ~second = {
    year; month; day; hour; minute; second
  }
end

module Where = struct
  type order_sign =
    | Equal
    | Less
    | Greater
    | LessOrEqual
    | GreaterOrEqual

  let string_of_order_sign sign = match sign with
    | Equal -> "="
    | Less -> "<"
    | Greater -> ">"
    | LessOrEqual -> "<="
    | GreaterOrEqual -> ">="

  type t =
    | Tag of string * string
    | Field of string * string
    | Time of order_sign * Datetime.t

  let string_of_t t = match t with
    | Tag(key, value) -> Printf.sprintf "%s='%s'" key value
    | Field(key, value) -> Printf.sprintf "%s='%s'" key value
    | Time(sign, date) -> Printf.sprintf "time %s '%s'" (string_of_order_sign sign) (Datetime.string_of_t date)

  let string_of_list_of_t list_t =
    String.concat " AND " (List.map string_of_t list_t)
end

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

  let string_of_value value = match value with
    | Float f -> string_of_float f
    | String s -> s
    | Bool b -> string_of_bool b
    | Int i -> string_of_int i

  let key_of_field (k, _) = k
  let value_of_field (_, v) = v

  let to_string (key, value) =
    Printf.sprintf
      "%s=%s"
      key
      (string_of_value value)
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
    is_basic: bool
  }

  let basic_retention_policy_of_string name = {
    name;
    duration = "";
    shard_group_duration = "";
    replicant = 0;
    default = false;
    is_basic = true;
  }

  let check_if_basic rp =
    if rp.is_basic
    then (failwith "This retention policy is a basic one. You can't only use it for the name")
    else ()

  let name_of_t t = t.name
  let duration_of_t t =
    check_if_basic t;
    t.duration
  let replicant_of_t t =
    check_if_basic t;
    t.replicant
  let is_default t =
    check_if_basic t;
    t.default
  let shard_group_duration_of_t t =
    check_if_basic t;
    t.shard_group_duration

  let t_of_tuple (name, duration, shard_group_duration, replicant, default) = {
    name;
    duration;
    shard_group_duration;
    replicant;
    default;
    is_basic = false;
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
    shard_group_duration;
    is_basic = false;
  }
end

module Measurement = struct
  (** The type of a measurement. *)
  type t = {
    name: string;
  }

  let t_of_string name = {
    name
  }

  let string_of_t m = m.name
end

module Point = struct
  (** The type of point. *)
  type t = {
    measurement: Measurement.t;
    fields: Field.t list;
    tags: Tag.t list;
    timestamp: int64 option;
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
    Printf.sprintf
      "%s,%s %s %s"
      (Measurement.string_of_t point.measurement)
      (String.concat "," (List.map Tag.to_string point.tags))
      (String.concat "," (List.map Field.to_string point.fields))
      (match point.timestamp with None -> "" | Some t -> Int64.to_string t)
end

module QueryResult = struct
  type serie = {
    name: string;
    columns: string list;
    values: Json.json list list;
  }

  type t = {
    statement_id: int;
    series: serie list
  }

  (* FIXME: Must return a list of t. I don't know why results is a list of
     object and not an object.
  *)
  let of_string str =
    let results =
      Json.from_string str |> Json.Util.member "results" |> Json.Util.to_list |> List.hd
    in
    let statement_id = Json.Util.member "statement_id" results |> Json.Util.to_int in
    let series = Json.Util.member "series" results |> Json.Util.to_list in
    let series = List.map
      (fun serie ->
         let name = Json.Util.member "name" serie |> Json.Util.to_string in
         let columns_list = Json.Util.member "columns" serie |> Json.Util.to_list in
         let columns = List.map Json.Util.to_string columns_list in
         let values_list = Json.Util.member "values" serie |> Json.Util.to_list in
         let values = List.map Json.Util.to_list values_list in
         {name; columns; values}
      )
      series
    in
    {statement_id; series}

  let statement_id_of_t r = r.statement_id
  let series_of_t r = r.series

  let values_of_serie s = s.values
  let columns_of_serie s = s.columns
  let name_of_serie s = s.name
end

module Client = struct
  type t = {
    username: string;
    password: string;
    host: string;
    port: int;
    use_https: bool;
    database: string
  }

  let create ?(username="root") ?(password="root") ?(host="localhost") ?(port=8086) ?(use_https=false) ~database () = {
      username; password; host; port; use_https; database
    }

  let switch_database client database = {
    username = client.username;
    password = client.password;
    host = client.host;
    port = client.port;
    use_https = client.use_https;
    database = database
  }

  (**********************************************************************)
  (***** Raw module *****)
  module Raw = struct
    let url client =
      let protocol = if client.use_https then "https" else "http" in
      Printf.sprintf
        "%s://%s:%d"
        protocol
        client.host
        client.port

    let get_request t ?(additional_params=[]) request =
      let base_url = url t in
      let additional_params = ("q", request) :: additional_params in
      let additional_params =
        String.concat "&" (List.map (fun (key, value) -> Printf.sprintf "%s=%s" key value) additional_params)
      in
      let url =
        Printf.sprintf
          "%s/query?%s"
          base_url
          additional_params
      in
      print_endline url;
      Cohttp_lwt_unix.Client.get (Uri.of_string url) >>= fun(response, body) ->
      let code = response |> Cohttp.Response.status |> Cohttp.Code.code_of_status in
      let body = Cohttp_lwt_body.to_string body in
      body

    let post_request client ?(additional_params=[]) data =
      let body_str =
        Printf.sprintf
          "q=%s"
          data
      in
      let body = Cohttp_lwt_body.of_string body_str in
      let base_url = url client in
      let additional_params =
        if List.length additional_params > 0
        then
          "?" ^ (String.concat "&" (List.map (fun (key, value) -> Printf.sprintf "%s=%s" key value) additional_params))
        else ""
      in
      let url = Printf.sprintf
          "%s/query%s"
          base_url
          additional_params
      in
      print_endline url;
      (* The headers are mandatory! Else, we will receive the error
          {"error":"missing required parameter \"q\""}
      *)
      let headers = Cohttp.Header.init () in
      let headers =
        Cohttp.Header.add headers "Content-Type" "application/x-www-form-urlencoded"
      in
      Cohttp_lwt_unix.Client.post ~body ~headers (Uri.of_string url) >>= fun (response, body) ->
      Cohttp_lwt_body.to_string body

    let get_points client ?retention_policy ?(where=[]) ?column ?group_by measurement =
      (* If a retention policy is given, we need to use %s.%s.%s. Else, we can
         only use the measurement and mention the database name in parameter *)
      let from_request = match retention_policy with
        | None -> (Measurement.string_of_t measurement)
        | Some rp -> Printf.sprintf
                       "%s.%s.%s"
                       client.database
                       (RetentionPolicy.name_of_t rp)
                       (Measurement.string_of_t measurement)
      in
      let column = match column with
        | None -> "*"
        | Some c -> c
      in
      let group_by = match group_by with
        | None -> ""
        | Some t -> Printf.sprintf "GROUP BY time(%s)" t
      in
      let where =
        if List.length where > 0
        then Printf.sprintf "WHERE %s" (Where.string_of_list_of_t where)
        else ""
      in
      let request =
        Printf.sprintf
          "SELECT %s FROM %s %s %s"
          column
          from_request
          where
          group_by
      in
      let additional_params = [("db", client.database)] in
      get_request client ~additional_params request

    let write_points client ?precision ?retention_policy points =
      let line = String.concat "\n" (List.map Point.line_of_point points) in
      let additional_params = match (retention_policy, precision) with
        | None, None -> []
        | Some rp, None -> [("rp", (RetentionPolicy.name_of_t rp))]
        | None, Some precision -> [("p", (Precision.string_of_t precision))]
        | Some rp, Some precision -> [("rp", RetentionPolicy.name_of_t rp); ("p", Precision.string_of_t precision)]
      in
      post_request client ~additional_params line

    let create_database client database_name =
      let str = Printf.sprintf
          "CREATE DATABASE %s"
          database_name
      in
      post_request client str

    let get_all_database_names client =
      get_request client "SHOW DATABASES"

    let get_all_retention_policies_of_database client =
      let str = Printf.sprintf
          "SHOW RETENTION POLICIES ON %s"
          client.database
      in
      get_request client str

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

    let get_all_measurements client =
      let query = "SHOW MEASUREMENTS" in
      let additional_params = [("db", client.database)] in
      get_request client ~additional_params query

    let get_tag_names_of_measurement client measurement =
      let query = "SHOW TAG KEYS" in
      let additional_params = [("db", client.database)] in
      get_request client ~additional_params query

    let get_field_names_of_measurement client measurement =
      let query = "SHOW FIELD KEYS" in
      let additional_params = [("db", client.database)] in
      get_request client ~additional_params query

  end
  (***** Raw module *****)
  (**********************************************************************)

  (** About databases *)
  let create_database client database_name =
    Raw.create_database client database_name >>= fun body_str ->
    Lwt.return ()

  let get_all_database_names client =
    (Raw.get_all_database_names client) >>= fun body_str ->
    let result = QueryResult.of_string body_str in
    let serie = QueryResult.series_of_t result |> List.hd in
    let values =
      List.map
        (fun value -> List.hd value |> Json.Util.to_string)
        (QueryResult.values_of_serie serie)
    in
    Lwt.return values

  let drop_database client name =
    Raw.drop_database client name >>= fun resp -> Lwt.return ()

  (** About retention policies *)
  let get_all_retention_policies client =
    Raw.get_all_retention_policies_of_database client >>= fun str ->
    let results = QueryResult.of_string str in
    (* Can be useful later to check the columns are the same than we need *)
    (* let columns = *)
    (*   Json.Util.member "columns" series *)
    (*   |> Json.Util.to_list *)
    (* in *)
    let values =
      List.map
        RetentionPolicy.t_of_json
        (QueryResult.values_of_serie (QueryResult.series_of_t results |> List.hd) |> List.hd)
    in
    Lwt.return values

  let create_retention_policy ?(default = false) ?(replicant=1) ~name ~duration client =
    Raw.create_retention_policy ~default ~replicant ~name ~duration client >>=
    fun resp -> Lwt.return ()

  let drop_retention_policy client name =
    Raw.drop_retention_policy client name >>= fun resp -> Lwt.return ()

  let get_default_retention_policy_of_database client =
    get_all_retention_policies client >>= fun rps ->
    Lwt.return (List.find (fun rp -> RetentionPolicy.is_default rp) rps)

  (** About points *)
  (* No raw implementation available due to the retention policy.
       Can be fixed by omitting the database and the rp in the request.
  *)
  let get_points client ?retention_policy ?(where=[]) ?column ?group_by measurement =
    Raw.get_points client ?retention_policy ~where ?column ?group_by measurement

  let write_points client ?precision ?retention_policy points =
    Raw.write_points client ?precision ?retention_policy points >>= fun resp -> Lwt.return ()

  (* TODO *)
  (* let write_raw_points client ?retention_policy raw_points = *)
    (* Lwt.return () *)

  let get_all_measurements client =
    Raw.get_all_measurements client >>= fun str ->
    let results = QueryResult.of_string str in
    let measurements =
      List.map
        (fun json -> Json.Util.to_string json |> Measurement.t_of_string)
        (QueryResult.values_of_serie (QueryResult.series_of_t results |> List.hd) |> List.hd)
    in
    (* let measurements = List.map Measurement.t_of_string raw_measurements in *)
    Lwt.return measurements

  (* Add a timestamp. Or maybe, use something more complicated. *)
  let get_tag_names_of_measurement client measurement =
    Raw.get_tag_names_of_measurement client measurement >>= fun str ->
    let results = QueryResult.of_string str in
    let series = QueryResult.series_of_t results in
    let tags_of_measurements =
      List.map
        (fun serie ->
           let name = QueryResult.name_of_serie serie in
           let tags =
             List.map
               (fun value -> List.hd value |> Json.Util.to_string)
               (QueryResult.values_of_serie serie)
           in
           (name, tags)
        )
        series
    in
    let values =
      snd @@ List.find
        (fun (name, values) -> String.equal name (Measurement.string_of_t measurement))
        tags_of_measurements
    in
    Lwt.return values

  let get_field_names_of_measurement client measurement =
    Raw.get_field_names_of_measurement client measurement >>= fun str ->
    let results = QueryResult.of_string str in
    let series = QueryResult.series_of_t results in
    let fields_of_measurements =
      List.map
        (fun serie ->
           let name = QueryResult.name_of_serie serie in
           let fields =
             List.map
               (fun value -> List.hd value |> Json.Util.to_string)
               (QueryResult.values_of_serie serie)
           in
           (name, fields)
        )
        series
    in
    let values =
      snd @@ List.find
        (fun (name, values) -> String.equal name (Measurement.string_of_t measurement))
        fields_of_measurements
    in
    Lwt.return values
end
