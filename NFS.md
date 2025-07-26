### NFS tag management scripts

##### Using extended attributes for tags [^1]

Add tags to a file

```shell
function addtag()
{ ## Add tag(s) to file.
    ## The specified tags are appended to the comma-separated string value of the user.xdg.tags namespace of extended attributes.
    ## Usage: addtag TAG [...] FILE
    tags_existing=$(getfattr -n user.xdg.tags --only-value "$argv[-1]")
    setfattr -n user.xdg.tags -v "${tags_existing},${(%j:,:)argv[1,-2]}" "$argv[-1]"
}
```

Find files by list of matching tags
==TODO==: translate this from `zsh` to target language.

```zsh
function findtag()
{ # Search for tags in the user.xdg.tags namespace of extended attributes.
      # Usage: findtag TAG [...] PATH
    find "$argv[-1]" -exec getfattr -n user.xdg.tags '{}' \+ 2>/dev/null | grep -B 1 -Pi "(?<=user.xdg.tags=\")(?=.*${(%j:)(?=.*:)argv[1,-2]})"
}
```

Cleanup and deduplicate tags
==TODO==: translate this from `zsh` to target language.

```zsh
function cleantag()
{ ## Clean the tag string of file(s).
    ## Tags are assumed to be substrings of the value of the user.xdg.tags extended attributes namespace.
    ## Usage: cleantag FILEPATH
    local tags_existing file
    for file in "$@"; do
    if getfattr -n user.xdg.tags --only-value $file &>/dev/null; then
        tags_existing=$(getfattr -n user.xdg.tags --only-value $file)
        tags_existing=${tags_existing//,,##/,} # Removing duplicate commas.
        tags_existing=$(echo "$tags_existing" | tr ',' '\n' | sort -u | tr '\n' ',') # Removing duplicate tags.
        tags_existing=${tags_existing/#,/}     # Removing leading comma.
        tags_existing=${tags_existing/%,/}     # Removing trailing comma.
        setfattr -n user.xdg.tags -v "${tags_existing}" $file
    fi
    done
}
```

Removing tags

```zsh
function removetag()
{ ## Remove tag(s) from file.
    ## The specified tags are removed from the comma-separated string value of the user.xdg.tags extended attributes namespace.
    ## Usage: removetag TAG [...] FILEPATH
    local tags_remaining tag
    setopt localoptions extendedglob
    if getfattr -n user.xdg.tags --only-value "$argv[-1]" &>/dev/null; then
    tags_remaining=$(getfattr -n user.xdg.tags --only-value "$argv[-1]")
    for tag in "$@[1,-2]"; do
        tags_remaining=${tags_remaining//$tag/}
        echo "Tag \"${tag}\" and its duplicates were removed."
    done
    tags_remaining=${tags_remaining//,,##/,} # Removing duplicate commas.
    tags_remaining=${tags_remaining/#,/}     # Removing leading comma.
    tags_remaining=${tags_remaining/%,/}     # Removing trailing comma.
    setfattr -n user.xdg.tags -v "${tags_remaining}" "$argv[-1]"
    fi
}
```
#### Potential rust libraries to use in reimplementing

- [xattr](https://docs.rs/xattr/latest/xattr/)
- [matress](https://lib.rs/crates/mattress)

[^1]: [Tagging files on the unix system](https://unix.stackexchange.com/a/781277)