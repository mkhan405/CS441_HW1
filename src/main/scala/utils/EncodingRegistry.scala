package utils

import com.knuddels.jtokkit.Encodings
import com.knuddels.jtokkit.api.{Encoding, EncodingRegistry, EncodingType}

val registry: EncodingRegistry = Encodings.newLazyEncodingRegistry()
val encoding: Encoding = registry.getEncoding(EncodingType.CL100K_BASE)