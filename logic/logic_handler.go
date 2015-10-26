package logic

import (
	"fmt"
	sparta "github.com/mweagle/Sparta"
	"net/http"
)

func LambdaLogic(event sparta.LambdaEvent, context sparta.LambdaContext, w http.ResponseWriter) {
	fmt.Fprintf(w, "LambdaLogic Data: %s", event)
}
