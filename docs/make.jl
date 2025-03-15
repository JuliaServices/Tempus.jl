using Documenter, Tempus

makedocs(modules = [Tempus], sitename = "Tempus.jl")

deploydocs(repo = "github.com/JuliaServices/Tempus.jl.git", push_preview = true)
