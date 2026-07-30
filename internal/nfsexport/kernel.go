package nfsexport

// Kernel constants for the sunrpc/nfsd cache channels. Values match Linux v6.8
// and nfs-utils 2.6.1.

// NFS export flags — include/uapi/linux/nfsd/export.h:29-45.
const (
	nfsexpReadOnly     = 0x0001  // NFSEXP_READONLY
	nfsexpInsecurePort = 0x0002  // NFSEXP_INSECURE_PORT
	nfsexpRootSquash   = 0x0004  // NFSEXP_ROOTSQUASH
	nfsexpAllSquash    = 0x0008  // NFSEXP_ALLSQUASH
	nfsexpNoSubtreeChk = 0x0400  // NFSEXP_NOSUBTREECHECK
	nfsexpFSID         = 0x2000  // NFSEXP_FSID
	nfsexpV4Root       = 0x10000 // NFSEXP_V4ROOT
)

// xprtsec transport-security mode bits — include/uapi/linux/nfsd/export.h:69-77.
// xprtsec_parse (fs/nfsd/export.c:1265-1286) rejects any mode > MTLS.
const (
	xprtsecNone = 0x0001 // NFSEXP_XPRTSEC_NONE
	xprtsecTLS  = 0x0002 // NFSEXP_XPRTSEC_TLS
	xprtsecMTLS = 0x0004 // NFSEXP_XPRTSEC_MTLS
)

// fsid types and their key lengths — fs/nfsd/nfsfh.h:117-211 (enum nfsd_fsid,
// key_len). The driver exports use an explicit numeric fsid (FSID_NUM), which
// pairs with the NFSEXP_FSID flag and a 4-byte big-endian fsid value.
const (
	fsidTypeNum        = 1 // FSID_NUM (fs/nfsd/nfsfh.h:120)
	fsidTypeUUID16     = 6
	fsidTypeUUID16Inum = 7
)

// FsidTypeNum exposes the FSID_NUM constant so callers outside the package
// (notably the agent reconciler tests) can construct fsid=0 lookups without
// duplicating the kernel ABI value.
func FsidTypeNum() int { return fsidTypeNum }

// keyLen returns the fsid byte length for a fsid type, mirroring key_len
// (fs/nfsd/nfsfh.h:199-212). A zero return marks an unsupported/invalid type.
func keyLen(fsidType int) int {
	switch fsidType {
	case 0: // FSID_DEV
		return 8
	case 1: // FSID_NUM
		return 4
	case 2: // FSID_MAJOR_MINOR
		return 12
	case 3: // FSID_ENCODE_DEV
		return 8
	case 4: // FSID_UUID4_INUM
		return 8
	case 5: // FSID_UUID8
		return 8
	case 6: // FSID_UUID16
		return 16
	case 7: // FSID_UUID16_INUM
		return 24
	default:
		return 0
	}
}
