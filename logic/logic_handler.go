package logic

import (
	"fmt"
	"github.com/Sirupsen/logrus"
	sparta "github.com/mweagle/Sparta"
	"net/http"
)

func LambdaLogic(event *sparta.LambdaEvent, context *sparta.LambdaContext, w *http.ResponseWriter, logger *logrus.Logger) {
	fmt.Fprintf(*w, "LambdaLogic Data: %s", event)
}
