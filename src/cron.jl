## Representation of a day of the week as a custom Dates.Period
struct DayOfWeek <: Dates.Period
    value::Int
end

## Returns the allowed range of values for different time periods
allowedrange(::Type{P}) where {P <: Union{Second, Minute}} = P(0):P(1):P(59)
allowedrange(::Type{Hour}) = Hour(0):Hour(1):Hour(23)
allowedrange(::Type{Day}) = Day(1):Day(1):Day(31)
allowedrange(::Type{Month}) = Month(1):Month(1):Month(12)
allowedrange(::Type{DayOfWeek}) = DayOfWeek(0):DayOfWeek(1):DayOfWeek(6)

function allowed end
function nextallowed end
function minimumallowed end
function valid end

abstract type CronField end

## Represents a wildcard (`*`) in a cron expression
struct Wildcard <: CronField end

Base.show(io::IO, ::Wildcard) = print(io, "*")

## Determines if a period `p` is allowed based on cron field definitions
allowed(::Period, ::Wildcard, ::Period, ::Wildcard) = true
allowed(p::Period, x::CronField, ::Period, ::Wildcard) = allowed(p, x)
allowed(::Period, ::Wildcard, q::Period, y::CronField) = allowed(q, y)
allowed(p::Period, x::CronField, q::Period, y::CronField) = allowed(p, x) || allowed(q, y)

allowed(p::P, ::Wildcard) where {P <: Period} = p in allowedrange(P)
valid(::Type{P}, ::Wildcard) where {P <: Period} = true

## Returns the next allowed value of period `p` according to cron field definitions
nextallowed(p::Period, ::Wildcard) = p

## Returns the minimum allowed value for different period types based on a wildcard definition
minimumallowed(::Type{P}, ::Wildcard) where {P <: Union{Second, Minute, Hour, DayOfWeek}} = P(0)
minimumallowed(::Type{P}, ::Wildcard) where {P <: Union{Day, Month}} = P(1)

## Represents a numeric value in a cron expression
struct Numeric <: CronField
    value::Int
end

Base.show(io::IO, x::Numeric) = print(io, x.value)

allowed(p::P, x::Numeric) where {P <: Period} = p == P(x.value)
nextallowed(_::P, x::Numeric) where {P <: Period} = P(x.value)
minimumallowed(::Type{P}, x::Numeric) where {P <: Period} = P(x.value)
valid(::Type{P}, x::Numeric) where {P <: Period} = P(x.value) in allowedrange(P)

## Represents a range of values in a cron expression (e.g., `5-10`)
struct Range <: CronField
    start::Int
    stop::Int
end

Base.show(io::IO, x::Range) = print(io, x.start, "-", x.stop)

allowed(p::P, x::Range) where {P <: Period} = P(x.start) <= p <= P(x.stop)
nextallowed(p::P, x::Range) where {P <: Period} = allowed(p, x) ? p : P(x.start)
minimumallowed(::Type{P}, x::Range) where {P <: Period} = P(x.start)
valid(::Type{P}, x::Range) where {P <: Period} = P(x.start) in allowedrange(P) && P(x.stop) in allowedrange(P)

## Represents a list of discrete values in a cron expression (e.g., `1,3,5`)
struct List <: CronField
    values::Vector{Int}
    List(values::Vector{Int}) = new(sort!(values))
end

Base.show(io::IO, x::List) = print(io, join(x.values, ","))

allowed(p::P, x::List) where {P <: Period} = Dates.value(p) in x.values
function nextallowed(p::P, x::List) where {P <: Period}
    for v in x.values
        P(v) > p && return P(v)
    end
    return P(x.values[1])
end
minimumallowed(::Type{P}, x::List) where {P <: Period} = P(x.values[1])
valid(::Type{P}, x::List) where {P <: Period} = all(P(v) in allowedrange(P) for v in x.values)

## Represents a stepped range (e.g., `*/5` or `1-10/2`): the allowed values are
## the range start, start+step, start+2step, ... within the range (standard cron)
struct Step <: CronField
    range::Union{Range, Wildcard}
    step::Int
end

Base.show(io::IO, x::Step) = print(io, x.range isa Wildcard ? "*" : "$(x.range.start)-$(x.range.stop)", "/", x.step)

allowed(p::P, x::Step) where {P <: Period} =
    allowed(p, x.range) && (p - minimumallowed(P, x.range)) % P(x.step) == P(0)
function nextallowed(p::P, x::Step) where {P <: Period}
    start = minimumallowed(P, x.range)
    p <= start && return start
    # the smallest on-step value >= p, or the start (signaling a wrap to the
    # caller) when that overshoots the range
    offset = (p - start) % P(x.step)
    v = offset == P(0) ? p : p + (P(x.step) - offset)
    return allowed(v, x) ? v : start
end
minimumallowed(::Type{P}, x::Step) where {P <: Period} = minimumallowed(P, x.range)
valid(::Type{P}, x::Step) where {P <: Period} = x.step >= 1 && valid(P, x.range) && P(x.step) in allowedrange(P)

## Represents a parsed cron expression with individual fields for each unit of time
struct Cron{S, M, H, D, Mo, DoW}
    second::S
    minute::M
    hour::H
    day_of_month::D
    month::Mo
    day_of_week::DoW
end

function Base.show(io::IO, c::Cron)
    print(io, "\"")
    show(io, c.second)
    print(io, " ")
    show(io, c.minute)
    print(io, " ")
    show(io, c.hour)
    print(io, " ")
    show(io, c.day_of_month)
    print(io, " ")
    show(io, c.month)
    print(io, " ")
    show(io, c.day_of_week)
    print(io, "\"")
end

# parse an individual cron expression field
# looking for wildcard, numeric value, list, range, or step
function parseCronField(field)
    # wildcard
    if field == "*"
        return Wildcard()
    end
    # single numeric value
    if occursin(r"^\d+$", field)
        return Numeric(parse(Int, field))
    end
    # range
    if occursin(r"^\d+(-\d+)?$", field)
        parts = split(field, "-")
        if length(parts) == 1
            return Numeric(parse(Int, parts[1]))
        else
            start = parse(Int, parts[1])
            stop = parse(Int, parts[2])
            start > stop && throw(ArgumentError("Invalid range: $field"))
            return Range(start, stop)
        end
    end
    # step
    if occursin(r"^(?:\*|\d+-\d+)(?:/\d+)$", field)
        parts = split(field, "/")
        return Step(parseCronField(parts[1]), parse(Int, parts[2]))
    end
    # list
    if occursin(r"^\d+(,\d+)+$", field)
        return List(parse.(Int, split(field, ",")))
    end
    throw(ArgumentError("Invalid cron field: $field"))
end

# @reboot has no equivalent here; jobs run on a schedule, not at scheduler start
const CRON_ALIASES = Dict(
    "@yearly"   => "0 0 1 1 *",
    "@annually" => "0 0 1 1 *",
    "@monthly"  => "0 0 1 * *",
    "@weekly"   => "0 0 * * 0",
    "@daily"    => "0 0 * * *",
    "@midnight" => "0 0 * * *",
    "@hourly"   => "0 * * * *",
)

const MONTH_NAMES = Dict(
    "JAN" => 1, "FEB" => 2, "MAR" => 3, "APR" => 4, "MAY" => 5, "JUN" => 6,
    "JUL" => 7, "AUG" => 8, "SEP" => 9, "OCT" => 10, "NOV" => 11, "DEC" => 12,
)

const DAY_NAMES = Dict(
    "SUN" => 0, "MON" => 1, "TUE" => 2, "WED" => 3, "THU" => 4, "FRI" => 5, "SAT" => 6,
)

# replace three-letter month/day names (case-insensitive) with their numeric values
function substitutenames(field::AbstractString, names::Dict{String, Int})
    return replace(field, r"[A-Za-z]+" => name -> begin
        value = get(names, uppercase(name), nothing)
        value === nothing && throw(ArgumentError("Invalid name \"$name\" in cron field: $field"))
        string(value)
    end)
end

# cron also numbers Sunday as 7; rewrite day-of-week fields so matching only
# ever sees 0. Ranges and steps reaching 7 become explicit value lists because
# wrapping 7 to 0 breaks their contiguity.
normalizedow(x::CronField) = x
normalizedow(x::Numeric) = x.value == 7 ? Numeric(0) : x
normalizedow(x::List) = 7 in x.values ? List(unique!(replace(x.values, 7 => 0))) : x
function normalizedow(x::Range)
    x.stop == 7 || return x
    x.start == 0 && return Range(0, 6)
    x.start == 7 && return Numeric(0)
    return List(unique!([0; x.start:6]))
end
function normalizedow(x::Step)
    (x.range isa Range && x.range.stop == 7 && x.step >= 1) || return x
    return List(unique!([v == 7 ? 0 : v for v in x.range.start:x.step:7]))
end

function parseCron(cron::AbstractString)
    expr = strip(cron)
    if startswith(expr, '@')
        expr = get(CRON_ALIASES, lowercase(expr), nothing)
        expr === nothing && throw(ArgumentError("Unknown cron alias: $cron"))
    end
    parts = split(expr)
    if length(parts) == 5
        minute, hour, day = map(parseCronField, parts[1:3])
        month = parseCronField(substitutenames(parts[4], MONTH_NAMES))
        day_of_week = parseCronField(substitutenames(parts[5], DAY_NAMES))
        second = Numeric(0)
    elseif length(parts) == 6
        second, minute, hour, day = map(parseCronField, parts[1:4])
        month = parseCronField(substitutenames(parts[5], MONTH_NAMES))
        day_of_week = parseCronField(substitutenames(parts[6], DAY_NAMES))
    else
        throw(ArgumentError("Invalid cron expression: $cron"))
    end
    day_of_week = normalizedow(day_of_week)
    # validate cron fields (seconds in range 0-59, minutes in range 0-59, hours in range 0-23, day of month in range 1-31, month in range 1-12, day of week in range 0-6)
    if !valid(Second, second)
        throw(ArgumentError("Invalid seconds value: $second"))
    end
    if !valid(Minute, minute)
        throw(ArgumentError("Invalid minutes value: $minute"))
    end
    if !valid(Hour, hour)
        throw(ArgumentError("Invalid hours value: $hour"))
    end
    if !valid(DayOfWeek, day_of_week)
        throw(ArgumentError("Invalid day of week value: $day_of_week"))
    end
    if !valid(Day, day)
        throw(ArgumentError("Invalid day of month value: $day"))
    end
    if !valid(Month, month)
        throw(ArgumentError("Invalid month value: $month"))
    end
    return Cron(second, minute, hour, day, month, day_of_week)
end

const ANY_CRON = Cron(Wildcard(), Wildcard(), Wildcard(), Wildcard(), Wildcard(), Wildcard())

# how far ahead getnext searches before concluding the expression can never fire
# (e.g. "0 0 30 2 *"); 10 years covers the worst gap between matches of any
# satisfiable expression, the 8 years between Feb 29ths around a non-leap
# century year
const MAX_LOOKAHEAD = Dates.Year(10)

# Compute the next trigger time after 'from'
#
# Each field is checked most-significant first; when a field doesn't match, time
# either jumps directly to the field's next allowed value (with all lesser
# fields reset to their minimums) or, when the field has to wrap, rolls over
# into the next larger unit via period arithmetic and re-enters the loop. All
# date construction is either period arithmetic or uses values already known to
# be valid, so carries at month/day/hour boundaries can't produce out-of-range
# `DateTime` arguments. Days advance one at a time because day-of-month,
# day-of-week, and month lengths interact; the month check above fast-forwards
# over months that can't match.
function getnext(cron::Cron, from::DateTime=Dates.now(UTC))
    minSeconds = minimumallowed(Second, cron.second)
    minMinutes = minimumallowed(Minute, cron.minute)
    limit = from + MAX_LOOKAHEAD
    next = trunc(from, Second) + Second(1)
    while true
        # Keep this diagnostic literal. Formatting a DateTime here pulls the
        # dynamic Dates display path into every compiled getnext call, even
        # when the search limit is not reached.
        next > limit && throw(ArgumentError(
            "cron expression does not fire within the 10-year search horizon",
        ))
        # check month
        curMonth = Month(next)
        if !allowed(curMonth, cron.month)
            nextMonth = nextallowed(curMonth, cron.month)
            # a next allowed month at or before the current one means we wrapped
            # into the next year; either way restart at the first day of that
            # month and let the checks below advance day/hour/minute/second
            y = nextMonth <= curMonth ? Year(next) + Year(1) : Year(next)
            next = DateTime(y, nextMonth, Day(1), Hour(0), Minute(0), Second(0))
            continue
        end
        # check day: when both day-of-month and day-of-week are restricted, a
        # day matching either one is allowed (standard cron semantics)
        curDay = Day(next)
        # cron numbers days 0=Sunday..6=Saturday; Dates.dayofweek gives 1=Monday..7=Sunday
        curDayOfWeek = DayOfWeek(dayofweek(next) % 7)
        if !allowed(curDay, cron.day_of_month, curDayOfWeek, cron.day_of_week)
            next = DateTime(Date(next) + Dates.Day(1))
            continue
        end
        # check hour
        curHour = Hour(next)
        if !allowed(curHour, cron.hour)
            nextHour = nextallowed(curHour, cron.hour)
            if nextHour <= curHour
                # wraps into the next day, which must be revalidated against the
                # date fields above
                next = DateTime(Date(next) + Dates.Day(1))
            else
                next = DateTime(Year(next), Month(next), curDay, nextHour, minMinutes, minSeconds)
            end
            continue
        end
        # check minute
        curMinute = Minute(next)
        if !allowed(curMinute, cron.minute)
            nextMinute = nextallowed(curMinute, cron.minute)
            if nextMinute <= curMinute
                next = trunc(next, Hour) + Hour(1)
            else
                next = DateTime(Year(next), Month(next), curDay, curHour, nextMinute, minSeconds)
            end
            continue
        end
        # check second
        curSecond = Second(next)
        if !allowed(curSecond, cron.second)
            nextSecond = nextallowed(curSecond, cron.second)
            if nextSecond <= curSecond
                next = trunc(next, Minute) + Minute(1)
            else
                next = DateTime(Year(next), Month(next), curDay, curHour, curMinute, nextSecond)
            end
            continue
        end
        # all fields are now valid/allowed, return
        return next
    end
end

"""
    getnext(cron::Cron, timezone::String, from::DateTime=Dates.now(UTC)) -> DateTime

Compute the next trigger time for `cron` interpreted in `timezone` (IANA name, e.g. `"America/Denver"`).
`from` is a UTC DateTime. Returns a UTC DateTime.
DST handling: spring-forward gaps are skipped, fall-back ambiguities use the first occurrence.
"""
function getnext(cron::Cron, timezone::String, from::DateTime=Dates.now(UTC))
    tz = TimeZone(timezone)
    # Convert UTC "from" to local time in the target timezone
    from_local = DateTime(ZonedDateTime(from, tz; from_utc=true))
    # Find next cron match in local time (reuses the existing UTC-agnostic algorithm)
    next_local = getnext(cron, from_local)
    # Convert back to UTC, handling DST transitions
    next_utc = _local_to_utc(next_local, tz, cron)
    # If `from` is in the second occurrence of a fall-back interval, choosing
    # the first occurrence of an ambiguous match puts it in the past. Skip the
    # rest of the repeated local interval; every ambiguous time in it has
    # already had its selected (first) occurrence.
    if next_utc <= from
        repeated_end = firstunambiguouslocal(next_local, tz)
        next_local2 = getnext(cron, repeated_end - Second(1))
        next_utc = _local_to_utc(next_local2, tz, cron)
    end
    return next_utc
end

function _local_to_utc(local_dt::DateTime, tz::TimeZone, cron::Cron)
    try
        zdt = ZonedDateTime(local_dt, tz)
        return DateTime(zdt, UTC)
    catch e
        if e isa TimeZones.NonExistentTimeError
            # Offset changes are not always one hour: Lord Howe advances by 30
            # minutes, and Apia skipped a whole day in 2011. Find the actual end
            # of this gap, then resume cron matching there.
            gap_end = firstvalidlocal(local_dt, tz)
            next_local = getnext(cron, gap_end - Second(1))
            return _local_to_utc(next_local, tz, cron)
        elseif e isa TimeZones.AmbiguousTimeError
            # Fall back: this local time occurs twice. Use first occurrence (before clocks change).
            zdt = ZonedDateTime(local_dt, tz, 1)
            return DateTime(zdt, UTC)
        else
            rethrow()
        end
    end
end

# Find the first valid local millisecond after a non-existent local time. This
# uses only the public ZonedDateTime constructor rather than TimeZones internals.
function firstvalidlocal(local_dt::DateTime, tz::TimeZone)
    step = Millisecond(1)
    upper = local_dt + step
    while !validlocal(upper, tz)
        step *= 2
        upper = local_dt + step
    end

    lower = local_dt
    while upper - lower > Millisecond(1)
        distance = Dates.value(upper - lower)
        middle = lower + Millisecond(distance ÷ 2)
        if validlocal(middle, tz)
            upper = middle
        else
            lower = middle
        end
    end
    return upper
end

function validlocal(local_dt::DateTime, tz::TimeZone)
    try
        ZonedDateTime(local_dt, tz)
        return true
    catch e
        e isa TimeZones.AmbiguousTimeError && return true
        e isa TimeZones.NonExistentTimeError && return false
        rethrow()
    end
end

# Find the end of a repeated local-time interval. The caller has already
# selected the first occurrence, so later matches inside the same ambiguous
# interval must not be returned as future times during the second occurrence.
function firstunambiguouslocal(local_dt::DateTime, tz::TimeZone)
    step = Millisecond(1)
    upper = local_dt + step
    while !unambiguouslocal(upper, tz)
        step *= 2
        upper = local_dt + step
    end

    lower = local_dt
    while upper - lower > Millisecond(1)
        distance = Dates.value(upper - lower)
        middle = lower + Millisecond(distance ÷ 2)
        if unambiguouslocal(middle, tz)
            upper = middle
        else
            lower = middle
        end
    end
    return upper
end

function unambiguouslocal(local_dt::DateTime, tz::TimeZone)
    try
        ZonedDateTime(local_dt, tz)
        return true
    catch e
        e isa TimeZones.AmbiguousTimeError && return false
        e isa TimeZones.NonExistentTimeError && return false
        rethrow()
    end
end
