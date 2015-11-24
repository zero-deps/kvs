package mws.kvs
package favorite

import mws.kvs.ImporterKvs
import mws.kvs.RingKvs

class FavoriteImporterRingKvs(val schemaName: String) extends ImporterKvs with RingKvs
