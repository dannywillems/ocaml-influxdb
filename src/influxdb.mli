module Json = Yojson.Basic

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
end

module Tag : sig
  (** The key of a tag. *)
  type key = string
  (** The value of a tag. *)
  type value = string

  (** A field is a couple (key, value) *)
  type t = key * value

  val of_key_and_value : key -> value -> t

  val to_string : t -> string
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

  val to_t : string -> RetentionPolicy.t  -> t

  val retention_policy_of_t : t -> RetentionPolicy.t

  val name_of_t : t -> string

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
end

module Client : sig

  type t

  (** Create a client *)
  val create :
    ?username:string -> ?password:string -> ?host:string -> ?port:int -> database:string -> unit -> t

  val switch_database : t -> string -> t

  module Raw : sig
    val get_all_database_names : t -> string Lwt.t

    val create_database : t -> string -> string Lwt.t

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

    val drop_database :
      t ->
      string ->
      string Lwt.t
  end

  val get_all_database_names : t -> string list Lwt.t

  val create_database : t -> string -> unit Lwt.t

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

  val drop_database :
    t ->
    string ->
    unit Lwt.t
  (** Write a list of points *)
  (* val write_points : t -> Point.t list -> unit *)

  (* val select : t -> Point.t list *)

  (* val get_all_tags_of_measurement : Client.t -> Measurement.t -> Tags.t list *)

  (* val get_all_fields_of_measurement : Client.t -> Measurement.t -> Fields.t list *)
end
