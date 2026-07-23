#ifndef DRAGON_SERIALIZABLE_TYPES
#define DRAGON_SERIALIZABLE_TYPES

#ifdef __cplusplus
extern "C" {
#endif

enum DragonSerType {
    SERTYPE_NONE = 0,
    SERTYPE_STR,
    SERTYPE_INT,
    SERTYPE_DOUBLE,
    SERTYPE_INTVECTOR,
    SERTYPE_DOUBLEVECTOR,
    SERTYPE_INTMATRIX,
    SERTYPE_DOUBLEMATRIX,
    SERTYPE_BYTEBUFFER
};

#ifdef __cplusplus
}
#endif

#endif