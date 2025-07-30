## bTagger: Backup tags for tiering and eviction

Tags are calculated for the follwing periods, given these input parameters:

```shell
btagger tags --every-n-hours 4 --minutes-offset 30 --lag-window-in-minutes 20
```

##### All time calculations are performed in UTC

- "**standard**": Every 4 hours, starting at midnight, offset by 30 minutes.
  - Transitions to **GLACIER_IR** after 24 hours.
  - Expires after 3 days.
  - _This tag is set on all backups, both manual and automated, to ensure that manual one-off backups are managed by the lifecycle configuration._
- "**nightly**": Nightly at 4:30am UTC.
  - Transitions to **GLACIER_IR** after 48 hours.
  - Expires after 7 days.
- "**weekly**": Weekly, on Saturday, at 4:30am UTC.
  - Transitions to **GLACIER_IR** after 7 days.
  - Expires after 1 month.
- "**monthly**": Monthly, on the last day of the month, at 4:30am UTC.
  - Transitions to **GLACIER_IR** after 1 month.
  - Transitions to **DEEP_ARCHIVE** after 2 months.
  - Expires after 6 months.
  - _This calculation considers leap-year and will run on the 29th of February when appropriate_
- "**quarterly**": Quarterly, on the last day of the quarter, at 4:30am UTC.
  - Transitions to **GLACIER_IR** after 3 months.
  - Transitions to **DEEP_ARCHIVE** after 6 months.
  - Expires after 1 year.
  - _March 31, June 30, September 30, and December 31_
- "**yearly**": Yearly, on December 31, at 4:30am UTC.
  - Transitions to **GLACIER_IR** after 1 year.
  - Transitions to **DEEP_ARCHIVE** after 2 years.
  - Expires after 3 years.

The tags can be formatted for use with S3 by default, but can be configured to output a custom key-value pair set for custom interoperability.

### S3 tiering and eviction is performed using s3 life-cycle policies. [^1] [^2]

```xml
<LifecycleConfiguration>
    <Rule>
        <ID>standard</ID>
        <Filter>
            <And>
                <Tag>
                    <Key>standard</Key>
                    <Value>1</Value>
                </Tag>
                <Tag>
                    <Key>nightly</Key>
                    <Value>0</Value>
                </Tag>
                <Tag>
                    <Key>weekly</Key>
                    <Value>0</Value>
                </Tag>
                <Tag>
                    <Key>monthly</Key>
                    <Value>0</Value>
                </Tag>
                <Tag>
                    <Key>quarterly</Key>
                    <Value>0</Value>
                </Tag>
                <Tag>
                    <Key>yearly</Key>
                    <Value>0</Value>
                </Tag>
            </And>
        </Filter>
        <Status>Enabled</Status>
        <Transition>
            <Days>1</Days>
            <StorageClass>GLACIER_IR</StorageClass>
        </Transition>
        <Expiration>
            <Days>3</Days>
        </Expiration>
    </Rule>
    <Rule>
        <ID>nightly</ID>
        <Filter>
            <And>
                <Tag>
                    <Key>nightly</Key>
                    <Value>1</Value>
                </Tag>
                <Tag>
                    <Key>weekly</Key>
                    <Value>0</Value>
                </Tag>
                <Tag>
                    <Key>monthly</Key>
                    <Value>0</Value>
                </Tag>
                <Tag>
                    <Key>quarterly</Key>
                    <Value>0</Value>
                </Tag>
                <Tag>
                    <Key>yearly</Key>
                    <Value>0</Value>
                </Tag>
            </And>
        </Filter>
        <Status>Enabled</Status>
        <Transition>
        <Transition>
            <Days>2</Days>
            <StorageClass>GLACIER_IR</StorageClass>
        </Transition>
        <Expiration>
            <Days>7</Days>
        </Expiration>
    </Rule>
    <Rule>
        <ID>weekly</ID>
        <Filter>
            <And>
                <Tag>
                    <Key>weekly</Key>
                    <Value>1</Value>
                </Tag>
                <Tag>
                    <Key>monthly</Key>
                    <Value>0</Value>
                </Tag>
                <Tag>
                    <Key>quarterly</Key>
                    <Value>0</Value>
                </Tag>
                <Tag>
                    <Key>yearly</Key>
                    <Value>0</Value>
                </Tag>
            </And>
        </Filter>
        <Status>Enabled</Status>
        <Transition>
            <Days>7</Days>
            <StorageClass>GLACIER_IR</StorageClass>
        </Transition>
        <Expiration>
            <Days>35</Days>
        </Expiration>
    </Rule>
    <Rule>
        <ID>monthly</ID>
        <Filter>
            <And>
                <Tag>
                    <Key>monthly</Key>
                    <Value>1</Value>
                </Tag>
                <Tag>
                    <Key>quarterly</Key>
                    <Value>0</Value>
                </Tag>
                <Tag>
                    <Key>yearly</Key>
                    <Value>0</Value>
                </Tag>
            </And>
        </Filter>
        <Status>Enabled</Status>
        <Transition>
            <Days>35</Days>
            <StorageClass>GLACIER_IR</StorageClass>
        </Transition>
        <Transition>
            <Days>95</Days>
            <StorageClass>DEEP_ARCHIVE</StorageClass>
        </Transition>
        <Expiration>
            <Days>190</Days>
        </Expiration>
    </Rule>
    <Rule>
        <ID>quarterly</ID>
        <Filter>
            <And>
                <Tag>
                    <Key>quarterly</Key>
                    <Value>1</Value>
                </Tag>
                <Tag>
                    <Key>yearly</Key>
                    <Value>0</Value>
                </Tag>
            </And>
        </Filter>
        <Status>Enabled</Status>
        <Transition>
            <Days>95</Days>
            <StorageClass>GLACIER_IR</StorageClass>
        </Transition>
        <Transition>
            <Days>190</Days>
            <StorageClass>DEEP_ARCHIVE</StorageClass>
        </Transition>
        <Expiration>
            <Days>370</Days>
        </Expiration>
    </Rule>
    <Rule>
        <ID>yearly</ID>
        <Filter>
            <And>
                <Tag>
                    <Key>yearly</Key>
                    <Value>0</Value>
                </Tag>
            </And>
        </Filter>
        <Status>Enabled</Status>
        <Transition>
            <Days>365</Days>
            <StorageClass>GLACIER_IR</StorageClass>
        </Transition>
        <Transition>
            <Days>730</Days>
            <StorageClass>DEEP_ARCHIVE</StorageClass>
        </Transition>
        <Expiration>
            <Days>1096</Days>
        </Expiration>
    </Rule>
    <Rule>
        <ID>cleanup</ID>
        <Status>Enabled</Status>
        <Expiration>
            <ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>
        </Expiration>
        <NoncurrentVersionExpiration>
            <NoncurrentDays>1</NoncurrentDays>    
        </NoncurrentVersionExpiration>
        <AbortIncompleteMultipartUpload>
          <DaysAfterInitiation>1</DaysAfterInitiation>
        </AbortIncompleteMultipartUpload>
    </Rule>
</LifecycleConfiguration>
```

[^1]: Due to the way AWS S3 service processes life-cycle rules, these need to be listed in sequnce of retention duration, as the last matching rule wins.

[^2]: This example includes a rule with ID "cleanup" that ensures non-current versions of objects are deleted permanently 1 day after becoming a non-current version. In this use case our intent is to remove the actual object version after a delete-marker becomes the most current version as a cause of our preceding expiration rules. The dangling delete-marker records are removed automatically via the `ExpiredObjectDeleteMarker` option. Additionally, we remove any incomplete multipart uploads older than 1 day.