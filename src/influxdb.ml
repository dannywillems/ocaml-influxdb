open Lwt.Infix

module Json = Yojson.Basic

(** timestamp -> datetime pour affichage *)
(** datetime -> timestamp pour les query *)
module Datetime = struct
  type t = {
    ptime_base: Ptime.t;
    nanosecond: int;
  }

  let big_bang = {
    ptime_base=Ptime.epoch;
    nanosecond=0
  }

  let int64_of_t t =
    let nanosecond_float = (Ptime.to_float_s t.ptime_base) *. 1e9 +. float_of_int t.nanosecond in
    Int64.of_float nanosecond_float


  let rec fill_with_zero max int =
    let rec nb_digit accu i =
      if i / 10 >= 1 then nb_digit (accu + 1) (i / 10) else accu
    in
    let nb_digit_int = nb_digit 1 int in
    if max - (nb_digit_int) > 0
    then String.init (max - nb_digit_int) (fun _ -> '0') ^ (string_of_int int)
    else string_of_int int

  let string_of_t t =
    let (year, month, day), ((hour, minute, second), offset) = Ptime.to_date_time t.ptime_base in
    Printf.sprintf
      "%s-%s-%sT%s:%s:%s.%sZ"
      (fill_with_zero 4 year)
      (fill_with_zero 2 month)
      (fill_with_zero 2 day)
      (fill_with_zero 2 hour)
      (fill_with_zero 2 minute)
      (fill_with_zero 2 second)
      (fill_with_zero 9 t.nanosecond)

  let query_string_of_t t =
    let (year, month, day), ((hour, minute, second), offset) = Ptime.to_date_time t.ptime_base in
    Printf.sprintf
      "%s-%s-%s %s:%s:%s"
      (fill_with_zero 4 year)
      (fill_with_zero 2 month)
      (fill_with_zero 2 day)
      (fill_with_zero 2 hour)
      (fill_with_zero 2 minute)
      (fill_with_zero 2 second)

  let to_t ~year ~month ~day ~hour ~minute ~second ~nanosecond =
    let date = (year, month, day) in
    let time = (hour, minute, second) in
    let ptime_base = Ptime.of_date_time (date, (time, 0)) in
    match ptime_base with
    | Some ptime_base -> { ptime_base; nanosecond }
    | None -> failwith "Error while creating Datetime.t. Be sure the parameters are in the ranges."

  let t_of_string str =
    let year = int_of_string (String.sub str 0 4) in
    let month = int_of_string (String.sub str 5 2) in
    let day = int_of_string (String.sub str 8 2) in
    let hour = int_of_string (String.sub str 11 2) in
    let minute = int_of_string (String.sub str 14 2) in
    let second = int_of_string (String.sub str 17 2) in
    let length_nano = String.length str - 20 - 1 in
    let nanosecond = int_of_string (String.sub str 20 length_nano) in
    to_t ~year ~month ~day ~hour ~minute ~second ~nanosecond
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
    | Time(sign, date) -> Printf.sprintf "time %s '%s'" (string_of_order_sign sign) (Datetime.query_string_of_t date)

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

  let value_of_json json = match json with
    | `Float f -> value_of_float f
    | `Int i -> value_of_int i
    | `String s -> value_of_string s
    | `Bool b -> value_of_bool b
    | _ -> failwith "Type not supported"

  let key_of_field (k, _) = k
  let value_of_field (_, v) = v

  let string_of_t (key, value) =
    Printf.sprintf
      "%s=%s"
      key
      (string_of_value value)

  let t_of_key_and_value key value = (key, value)
end

module Tag = struct
  (** The key of a tag. *)
  type key = string
  (** The value of a tag. *)
  type value = string

  (** A field is a couple (key, value) *)
  type t = key * value

  let t_of_key_and_value k v = (k, v)

  let string_of_t (key, value) =
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
    time: Datetime.t;
  }

  type raw_value =
    | Field of Field.t
    | Tag of Tag.t
    | Time of Datetime.t

  (* FIXME: the timestamp must be the current time, in microseconds/nanoseconds *)
  let to_t measurement fields tags time =
    {measurement; fields; tags; time}

  (** Get the measurement of a point. *)
  let measurement_of_point point = point.measurement

  (** Get the fields of a point. *)
  let fields_of_point point = point.fields

  (** Get the tags of a point. *)
  let tags_of_point point = point.tags

  (** Get the timestamp of the point. *)
  let time_of_point point = point.time

  let line_of_t point =
    Printf.sprintf
      "%s,%s %s %s"
      (Measurement.string_of_t point.measurement)
      (String.concat "," (List.map Tag.string_of_t point.tags))
      (String.concat "," (List.map Field.string_of_t point.fields))
      (Int64.to_string (Datetime.int64_of_t point.time))

  let to_t measurement fields tags time = {
    measurement; fields; tags; time
  }

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
    (* Support the case series is empty. e.g. when there is no measurement. *)
    let series = match (Json.Util.member "series" results) with
      | `List l -> l
      | _ -> []
    in
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

  let database_of_t c = c.database

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
      Cohttp_lwt_unix.Client.get (Uri.of_string url) >>= fun(response, body) ->
      (* let code = response |> Cohttp.Response.status |> Cohttp.Code.code_of_status in *)
      let body = Cohttp_lwt_body.to_string body in
      body

    let post_request client endpoint ?(additional_params=[]) data =
      let body = Cohttp_lwt_body.of_string data in
      let base_url = url client in
      let additional_params =
        if List.length additional_params > 0
        then
          "?" ^ (String.concat "&" (List.map (fun (key, value) -> Printf.sprintf "%s=%s" key value) additional_params))
        else ""
      in
      let url = Printf.sprintf
          "%s/%s%s"
          base_url
          endpoint
          additional_params
      in
      (* The headers are mandatory! Else, we will receive the error
          {"error":"missing required parameter \"q\""}
      *)
      let headers = Cohttp.Header.init () in
      let headers =
          Cohttp.Header.add headers "Content-Type" "application/x-www-form-urlencoded"
      in
      Cohttp_lwt_unix.Client.post ~body ~headers (Uri.of_string url) >>= fun (response, body) ->
      (* let code = response |> Cohttp.Response.status |> Cohttp.Code.code_of_status in *)
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
      let line = String.concat "\n" (List.map Point.line_of_t points) in
      let additional_params = match (retention_policy, precision) with
        | None, None -> []
        | Some rp, None -> [("rp", (RetentionPolicy.name_of_t rp))]
        | None, Some precision -> [("epoch", (Precision.string_of_t precision))]
        | Some rp, Some precision -> [("rp", RetentionPolicy.name_of_t rp); ("epoch", Precision.string_of_t precision)]
      in
      let additional_params = ("db", client.database) :: additional_params in
      post_request client "write" ~additional_params line

    let create_database client database_name =
      let query = Printf.sprintf
          "q=CREATE DATABASE %s"
          database_name
      in
      post_request client "query" query

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
      let query = Printf.sprintf
          "q=DROP DATABASE %s"
          name
      in
      post_request client "query" query

    let create_retention_policy ?(default = false) ?(replicant=1) ~name ~duration client =
      let str_default = if default then "DEFAULT" else "" in
      let query = Printf.sprintf
          "q=CREATE RETENTION POLICY %s \
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
      post_request client "query" query

    let get_all_measurements client =
      let query = "SHOW MEASUREMENTS" in
      let additional_params = [("db", client.database)] in
      get_request client ~additional_params query

    let drop_measurement client measurement =
      (* FIXME: Need to escape double quotes. For the moment, we can't drop the
         measurement q=cpu_load_short due to q=. Same thing happens when we use
         simple quote.
      *)
      let query =
        Printf.sprintf
          "q=DROP MEASUREMENT %s"
          (Measurement.string_of_t measurement)
      in
      let additional_params = [("db", client.database)] in
      post_request client "query" ~additional_params query

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


  let write_points client ?precision ?retention_policy points =
    Raw.write_points client ?precision ?retention_policy points >>= fun resp -> Lwt.return_unit

  let get_all_measurements client =
    Raw.get_all_measurements client >>= fun str ->
    let results = QueryResult.of_string str in
    let measurements = match (QueryResult.series_of_t results) with
      | [] -> []
      | head :: tail ->
        List.map
          (fun json -> Json.Util.to_string json |> Measurement.t_of_string)
          ((QueryResult.values_of_serie head) |> List.hd)
          (* let measurements = List.map Measurement.t_of_string raw_measurements in *)
    in
    Lwt.return measurements

  let drop_measurement client measurement =
    Raw.drop_measurement client measurement >>= fun resp -> Lwt.return ()

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

  let get_field_position tag_names field_names timestamp_name columns =
    List.mapi
      (fun pos name ->
         let equal_name = String.equal name in
         if equal_name timestamp_name
         then (0,"time")
         else
           try
             let field_name = List.find equal_name field_names in
             (1, field_name)
           with Not_found ->
             let tag_name = List.find equal_name tag_names in
             (2, tag_name)
      )
      columns

  let rec get_point_of_value accu point info =
    match info with
    | [] -> accu
    | head :: tail ->
      let type_value, name = head in
      let current_value_of_point = List.hd point in
      let value =
        match type_value with
        | 0 -> Point.Time (Datetime.t_of_string (Json.Util.to_string current_value_of_point))
        | 1 -> Point.Field (name, (Field.value_of_json current_value_of_point))
        | 2 -> (Point.Tag (Tag.t_of_key_and_value name (Json.Util.to_string current_value_of_point)))
        | _ -> failwith "get_point_of_value, type_value is not 0, 1 or 2"
      in
      get_point_of_value (value :: accu) (List.tl point) tail

  let rec point_of_raw_point point raw_point = match raw_point with
    | [] -> point
    | head :: tail ->
      let point = match head with
        | Point.Time t ->
          Point.to_t
            (Point.measurement_of_point point)
            (Point.fields_of_point point)
            (Point.tags_of_point point)
            t
        | Point.Tag t ->
          Point.to_t
            (Point.measurement_of_point point)
            (Point.fields_of_point point)
            (t :: (Point.tags_of_point point))
            (Point.time_of_point point)
        | Point.Field t ->
          Point.to_t
            (Point.measurement_of_point point)
            (t :: (Point.fields_of_point point))
            (Point.tags_of_point point)
            (Point.time_of_point point)
      in
      point_of_raw_point point tail

    (** 0, name, pos or 1, name, pos or 2, name, pos *)
        (* name, 0, values or name, 1, values *)

  (** About points *)
  let get_points client ?retention_policy ?(where=[]) ?column ?group_by measurement =
    (* IMPROVEME *)
    let field_names = get_field_names_of_measurement client measurement in
    let tag_names = get_tag_names_of_measurement client measurement in
    field_names >>= fun field_names ->
    tag_names >>= fun tag_names ->
    Raw.get_points client ?retention_policy ~where ?column ?group_by measurement >>= fun points ->
    let results = QueryResult.of_string points in
    let serie = QueryResult.series_of_t results in
    let points = match serie with
    | [] -> []
    | serie :: tail ->
      let columns = QueryResult.columns_of_serie serie in
      let info = get_field_position tag_names field_names "time" columns in
      (* this function returns unit and iterate over a *)
      (* ((1, field_name, i), (2, tag_name, j)) -> 1 for field, 2 for tag. 0 is used for timestamp. *)
      let points =
        List.map
          (fun point -> get_point_of_value [] point info)
          (QueryResult.values_of_serie serie)
      in
      List.map
        (fun raw_point -> point_of_raw_point (Point.to_t measurement [] [] Datetime.big_bang) raw_point)
        points
    in
    Lwt.return points
end
