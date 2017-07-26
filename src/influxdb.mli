module Json = Yojson.Basic

module Datetime : sig
  type t

  val string_of_t : t -> string

  val query_string_of_t : t -> string

  val t_of_string : string -> t

  val to_t : year:int -> month:int -> day:int -> hour:int -> minute:int -> second:int -> nanosecond:int -> t

  val string_of_t : t -> string
end


module Where : sig
  type order_sign =
    | Equal
    | Less
    | Greater
    | LessOrEqual
    | GreaterOrEqual

  type t =
    | Tag of string * string
    | Field of string * string
    | Time of order_sign * Datetime.t
end

module Precision : sig
  type t =
    | Second
    | Millisecond
    | Microsecond
    | Nanosecond

  val string_of_t : t -> string

  val t_of_string : string -> t
end

module Field : sig
  (** The key of a field is a string. *)
  type key = string

  (** A value is either a float, a string, a boolean of an integer. *)
  type value =
    | Float of float
    | String of string
    | Bool of bool
    | Int of int

  (** A field is a couple (key, value) *)
  type t = key * value

  val value_of_string : string -> value
  val value_of_bool : bool -> value
  val value_of_float : float -> value
  val value_of_int : int -> value

  (** Get the key of a tag. *)
  val key_of_field : t -> key
  (** Get the value of a tag. *)
  val value_of_field : t -> value

  val string_of_t : t -> string

  val t_of_key_and_value : key -> value -> t
end

module Tag : sig
  (** The key of a tag. *)
  type key = string
  (** The value of a tag. *)
  type value = string

  (** A field is a couple (key, value) *)
  type t = key * value

  val t_of_key_and_value : key -> value -> t

  val string_of_t : t -> string
end

module RetentionPolicy : sig
  type t

  val name_of_t : t -> string
  val duration_of_t : t -> string
  val replicant_of_t : t -> int
  val shard_group_duration_of_t : t -> string
  val is_default : t -> bool

  val t_of_tuple : string * string * string * int * bool -> t
  val t_of_json : Json.json -> t

  val to_t :
    name:string ->
    duration:string ->
    shard_group_duration:string ->
    replicant:int ->
    default:bool ->
    t
end

module Measurement : sig
  (** The type of a measurement. *)
  type t

  val t_of_string : string -> t

  val string_of_t : t -> string
end

(** About points. *)
module Point : sig
  (** The type of point. *)
  type t

  (** Get the fields of a point. *)
  val fields_of_point : t -> Field.t list

  (** Get the tags of a point. *)
  val tags_of_point : t -> Tag.t list
  (** Get the measurement of a point. *)
  val measurement_of_point : t -> Measurement.t

  val time_of_point : t -> Datetime.t

  val to_t : Measurement.t -> Field.t list -> Tag.t list -> Datetime.t -> t

  val line_of_t : t -> string
end

(** Represent the result of a query *)
module QueryResult : sig
  (** A result is a statement id bounded to a list of series *)
  type t

  (** A serie contains a name, the columns of the result and the corresponding
      list of values.
      The values order is the same than the columns order.
      IMPROVEME: merge columns and values and return a list of list of (column, value)?
  *)
  type serie

  val series_of_t : t -> serie list
  val statement_id_of_t : t -> int

  val name_of_serie : serie -> string
  val columns_of_serie : serie -> string list
  val values_of_serie : serie -> Json.json list list
end

module Client : sig

  type t

  val database_of_t : t -> string

  (** Create a client *)
  val create :
    ?username:string -> ?password:string -> ?host:string -> ?port:int -> ?use_https:bool -> database:string -> unit -> t

  val switch_database : t -> string -> t

  module Raw : sig
    val get_request : t -> ?additional_params:(string * string) list -> string -> string Lwt.t

    val post_request : t -> string -> ?additional_params:(string * string) list -> string -> string Lwt.t

    (** About databases *)
    val get_all_database_names : t -> string Lwt.t

    val create_database : t -> string -> string Lwt.t

    val drop_database :
      t ->
      string ->
      string Lwt.t

    (** About retention policies *)
    val get_all_retention_policies_of_database : t -> string Lwt.t

    val create_retention_policy :
      ?default:bool ->
      ?replicant:int ->
      name:string ->
      duration:string ->
      t ->
      string Lwt.t

    val drop_retention_policy :
      t ->
      string ->
      string Lwt.t

    val get_points :
      t ->
      ?retention_policy:RetentionPolicy.t ->
      ?where:Where.t list ->
      ?column:string ->
      ?group_by:string ->
      Measurement.t ->
      string Lwt.t

    val get_all_measurements :
      t -> string Lwt.t

    val drop_measurement : t -> Measurement.t -> string Lwt.t

    val write_points :
      t ->
      ?precision:Precision.t ->
      ?retention_policy:RetentionPolicy.t ->
      Point.t list ->
      string Lwt.t

    val get_tag_names_of_measurement : t -> Measurement.t -> string Lwt.t

    val get_field_names_of_measurement : t -> Measurement.t -> string Lwt.t
  end

  val get_default_retention_policy_of_database : t -> RetentionPolicy.t Lwt.t

  (** About databases *)
  val get_all_database_names : t -> string list Lwt.t

  val create_database : t -> string -> unit Lwt.t

  val drop_database :
    t ->
    string ->
    unit Lwt.t

  (** About retention policies *)
  val get_all_retention_policies : t -> RetentionPolicy.t list Lwt.t

  val create_retention_policy :
    ?default:bool ->
    ?replicant:int ->
    name:string ->
    duration:string ->
    t ->
    unit Lwt.t

  val drop_retention_policy :
    t ->
    string ->
    unit Lwt.t

  val get_points :
    t ->
    ?retention_policy:RetentionPolicy.t ->
    ?where:Where.t list ->
    ?column:string ->
    ?group_by:string ->
    Measurement.t ->
    Point.t list Lwt.t

  val write_points :
    t ->
    ?precision:Precision.t ->
    ?retention_policy:RetentionPolicy.t ->
    Point.t list ->
    unit Lwt.t

  (** About measurements *)
  val get_all_measurements : t -> Measurement.t list Lwt.t

  val drop_measurement : t -> Measurement.t -> unit Lwt.t

  val get_tag_names_of_measurement : t -> Measurement.t -> string list Lwt.t

  val get_field_names_of_measurement : t -> Measurement.t -> string list Lwt.t
end
